package com.amazon.deequ.analyzers.jdbc

import com.amazon.deequ.analyzers.Entropy
import com.amazon.deequ.analyzers.jdbc.JdbcUtils.{connectionProperties, jdbcUrl, withSpark}

object EntropyWithSpark extends App {
  withSpark { session =>

    val data = session.read.jdbc(jdbcUrl, "food_des", connectionProperties())

    val entropyOfFatFactor = Entropy("fat_factor").calculate(data)

    println(entropyOfFatFactor)
  }
}
