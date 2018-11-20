package com.amazon.deequ.analyzers.jdbc

import com.amazon.deequ.analyzers.jdbc.JdbcUtils.withJdbc

object EntropyWithJdbc extends App {
  withJdbc { connection =>

    val table = Table("food_des", connection)

    val entropyOfFatFactor = JdbcEntropy("fat_factor").calculate(table)

    println(entropyOfFatFactor)

  }
}
