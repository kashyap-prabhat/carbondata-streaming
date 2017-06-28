package org.apache.spark.sql.exceptions


class InvalidSchemaException(val message: String) extends Exception {

  Console.println(message + "Schema validation failed")
}
