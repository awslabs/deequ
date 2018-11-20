package com.amazon.deequ.analyzers.jdbc

import com.amazon.deequ.analyzers.jdbc.JdbcUtils.withJdbc

object UniqueValueRatioWithJdbc extends App {
  withJdbc { connection =>

    val table = Table("food_des", connection)

    val uniqueValueRatioOfFatFactor = JdbcUniqueValueRatio("fat_factor").calculate(table)

    println(uniqueValueRatioOfFatFactor)
  }
}
