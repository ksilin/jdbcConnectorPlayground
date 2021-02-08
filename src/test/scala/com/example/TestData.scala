package com.example

object TestData {

  case class Address(street: String, building: String, index: String)
  case class User(id: String, name: String, address: Address, changedAt: Long)

  case class Product(id: String, description: String)
  val productTableDDl: String => String = { tableName =>
    s"""$tableName (
       | id varchar(255) NOT NULL,
       | description varchar(255),
       | PRIMARY KEY(id)
       |)""".stripMargin
  }

  case class ProductOptional(id: Option[String] = None, description: Option[String] = None)
  case class Order(
      id: String,
      userId: String,
      prodId: String,
      amount: Int,
      location: String,
      timestamp: Long
  )
  case class Shipment(id: String, orderId: String, warehouse: String, timestamp: Long)
  case class Click(userId: String, element: String, userAgent: String, timestamp: Long)
}
