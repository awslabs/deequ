/**
  * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
  *
  * Licensed under the Apache License, Version 2.0 (the "License"). You may not
  * use this file except in compliance with the License. A copy of the License
  * is located at
  *
  *     http://aws.amazon.com/apache2.0/
  *
  * or in the "license" file accompanying this file. This file is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
  * express or implied. See the License for the specific language governing
  * permissions and limitations under the License.
  *
  */

package com.amazon.deequ.analyzers.jdbc

import java.sql.{Connection, DriverManager}

import org.sqlite.Function

/**
  * TODO
  * To be mixed with Tests so they can use a default spark context suitable for testing
  */
trait JdbcContextSpec {

  val jdbcUrl = "jdbc:sqlite:"

  def withJdbc(testFunc: Connection => Unit): Unit = {
    classOf[org.postgresql.Driver]
    val connection = DriverManager.getConnection(jdbcUrl)

    // Register user defined function for regular expression matching
    Function.create(connection, "regexp_matches", new Function() {
      protected def xFunc(): Unit = {
        val textToMatch = value_text(0)
        val pattern = value_text(1).r

        pattern.findFirstMatchIn(textToMatch) match {
          case Some(_) => result(1) // If a match is found, return any value other than NULL
          case None => result() // If no match is found, return NULL
        }
      }
    })

    try {
      testFunc(connection)
    } finally {
      connection.close()
    }
  }

}
