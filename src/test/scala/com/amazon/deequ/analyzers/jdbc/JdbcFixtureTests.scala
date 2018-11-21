package com.amazon.deequ.analyzers.jdbc

import org.scalatest.{Matchers, WordSpec}

class JdbcFixtureTests
  extends WordSpec with Matchers with JdbcContextSpec with JdbcFixtureSupport {

  "has Table" should {
    "return false on non-existing table" in withJdbc{ connection =>
      assert(!hasTable(connection, "myTable"))
    }

    "return true on existing table" in withJdbc { connection =>
      val table = getTableWithNumericValues(connection)

      assert(hasTable(connection, table.name))
    }
  }
}


