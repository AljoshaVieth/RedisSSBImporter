package de.aljoshavieth.redisssbinserter

import redis.clients.jedis.{Jedis, JedisPool, JedisPooled, Pipeline}

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.io.Source
import scala.util.{Failure, Success, Try}


val customerStructure: Array[String] =
  Array("c_custkey", "c_name", "c_address", "c_city", "c_nation", "c_region", "c_phone", "c_mktsegment")
val partStructure: Array[String] =
  Array("p_partkey", "p_name", "p_mfgr", "p_category", "p_brand1", "p_color", "p_type", "p_size", "p_container")
val dateStructure: Array[String] =
  Array("d_datekey", "d_date", "d_dayofweek", "d_month", "d_year", "d_yearmonthnum", "d_yearmonth", "d_daynuminweek", "d_daynuminmonth", "d_daynuminyear", "d_monthnuminyear", "d_weeknuminyear", "d_sellingseason", "d_lastdayinweekfl", "d_lastdayinmonthfl", "d_holidayfl", "d_weekdayfl")
val supplierStructure: Array[String] =
  Array("s_suppkey", "s_name", "s_address", "s_city", "s_nation", "s_region", "s_phone")
val lineorderStructure: Array[String] =
  Array("lo_orderkey", "lo_linenumber", "lo_custkey", "lo_partkey", "lo_suppkey", "lo_orderdate", "lo_orderpriority", "lo_shippriority", "lo_quantity", "lo_extendedprice", "lo_ordtotalprice", "lo_discount", "lo_revenue", "lo_supplycost", "lo_tax", "lo_commitdate", "lo_shipmod")


val pool: JedisPool = new JedisPool("localhost", 6380)
val jedis: Jedis = pool.getResource
val pipeline: Pipeline = jedis.pipelined()

@main
def main(): Unit = {
  jedis.flushAll()
  setRedisConfig()
  val startTime = System.nanoTime
  println("Inserting data...")
  parseData()
  println("\nAll data inserted in " + (System.nanoTime() - startTime) + " nanoseconds")
  resetRedisConfig()
  jedis.close()
  pool.close()
}

def parseData(): Unit = {
  readLines("date.tbl", dateStructure)
  readLines("customer.tbl", customerStructure)
  readLines("supplier.tbl", supplierStructure)
  readLines("part.tbl", partStructure)
  readLines("lineorder.tbl", lineorderStructure)
}

def readLines(fileName: String, dataStructure: Array[String]): Unit = {
  val tableName: String = fileName.split("\\.")(0);
  val startTime = System.nanoTime
  println("\nInserting data into " + tableName)
  Try(Source.fromFile(fileName)) match {
    case Success(file) =>
      file.getLines().grouped(1000).foreach(s => insertIntoRedis(s, tableName, dataStructure, tableName == "lineorder"))
      println("Inserted data into " + tableName + " in " + (System.nanoTime() - startTime) + " nanoseconds")
      file.close()
    case Failure(exception) =>
      println("Could not read file `" + fileName + "`. It should be placed into the same folder as the .jar")
      println(exception.getMessage)
  }
}

def insertIntoRedis(data: Seq[String], databaseName: String, dataStructure: Array[String], isLineOrder: Boolean): Unit = {
  data.foreach(line => {
    val values = line.split("\\|")
    val valueIterator = values.iterator
    dateStructure.foreach(key => {
      if (valueIterator.hasNext) {
        if (isLineOrder) {
          pipeline.hset(databaseName + ":" + values(0) + ":" + values(1), key, valueIterator.next())
        } else {
          pipeline.hset(databaseName + ":" + values(0), key, valueIterator.next())
        }
      }
    })
  })
  pipeline.sync()
}

def setRedisConfig(): Unit = {
  jedis.configSet("appendonly", "no")
  jedis.configSet("save", "")
}

def resetRedisConfig(): Unit = {
  jedis.configSet("appendonly", "yes")
  jedis.configSet("save", "900 1")
}
