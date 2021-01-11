package com.example

import io.getquill._
import wvlet.log.LogSupport

import java.sql.ResultSet

case class DescribeResult(
    Field: String,
    `Type`: String,
    `Null`: String,
    Key: Option[String],
    Default: String,
    Extra: Option[String]
)

case class JdbcHelper(ctx: MysqlJdbcContext[SnakeCase.type]) extends LogSupport {

  import ctx._

  val describeResultExtractor: ResultRow => DescribeResult = (rr: ResultRow) => {
    val key: Option[String] = {
      val k = rr.getString("Key")
      if (k.isBlank) None else Some(k)
    }

    val extra: Option[String] = {
      val k = rr.getString("Extra")
      if (k.isBlank) None else Some(k)
    }
    DescribeResult(
      rr.getString("Field"),
      rr.getString("Type"),
      rr.getString("Null"),
      key,
      rr.getString("Default"),
      extra
    )
  }

  def doesTableExist(tableName: String): Boolean = {
    val tables =
      ctx.executeQuery(s"SHOW TABLES LIKE '%$tableName%';", extractor = rr => rr.getString(1))
    tables.contains(tableName)
  }

  def doesTableExist2(tableName: String, dbName: Option[String] = None): Boolean = {

    val queryPrefix = "SELECT COUNT(*) FROM information_schema.tables WHERE "
    val querySuffix = s"table_name = '$tableName'"

    val fullQuery =
      queryPrefix + dbName.map(db => s"table_schema = '$dbName' AND ").getOrElse("") + querySuffix
    val res: List[Int] = ctx.executeQuery(fullQuery, extractor = (rr: ResultRow) => rr.getInt(1))
    res.headOption.exists(c => c > 0)
  }

  def listTables(dbName: Option[String] = None): List[String] = {
    val queryPrefix = "SELECT table_name FROM information_schema.tables"
    val fullQuery =
      queryPrefix + dbName.map(db => s" WHERE  table_schema = '$dbName'").getOrElse("")
    ctx.executeQuery(fullQuery, extractor = (rr: ResultRow) => rr.getString(1))
  }

  def dropTable(tableName: String): Long =
    ctx.executeAction(s"DROP TABLE IF EXISTS $tableName")

  def describeTable(tableName: String): List[DescribeResult] =
    ctx.executeQuery[DescribeResult](
      s"describe $tableName",
      extractor = describeResultExtractor
    )

  def logTableMetadata(tableName: String): Unit = {
    info(s"logging table $tableName")
    ctx.executeQuery[Unit](
      s"describe $tableName",
      extractor = metadataExtractor
    )
  }

  def getGtid: String = {
    val showVarsSql =
      s"""SHOW GLOBAL VARIABLES LIKE 'gtid_executed'""".stripMargin
    val variables: List[(String, String)] = ctx.executeQuery[(String, String)](showVarsSql, extractor = variableExtractor)
    variables.head._2.takeWhile(_ != ':')
  }

  val metadataExtractor: ResultSet => Unit = (rr: ResultRow) => {
    val meta     = rr.getMetaData
    val colCount = meta.getColumnCount
    info("---")
    (1 to colCount) foreach { colNum: Int =>
      info(s"column index: $colNum")
      info("catalog name: " + meta.getCatalogName(colNum))
      info("schema name: " + meta.getSchemaName(colNum))
      info("table name: " + meta.getTableName(colNum))
      info("column label: " + meta.getColumnLabel(colNum))
      info("column name: " + meta.getColumnName(colNum))
      info("column class name: " + meta.getColumnClassName(colNum))
      info("column type: " + meta.getColumnType(colNum).toString)
      info("column type name: " + meta.getColumnTypeName(colNum))
    }
  }

  val variableExtractor: ResultSet => (String, String) = (rr: ResultRow) => {
    (rr.getString("Variable_name"), rr.getString("Value"))
  }

}
