package de.aljoshavieth.redisssbinserter
package classicStructure

import redis.clients.jedis.search.{IndexDefinition, IndexOptions, Schema}
import redis.clients.jedis.{Jedis, JedisPool, JedisPooled, Pipeline}

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.io.Source
import scala.util.{Failure, Success, Try}

object InsertWithClassicStructure extends Inserter {
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


	override def execute(jedisPooled: JedisPooled, jedis: Jedis): Unit = {
		jedis.flushAll()
		val startTime = System.nanoTime
		setRedisConfig(jedis)
		//setIndexes()
		println("Inserting data...")
		val pipeline: Pipeline = jedisPooled.pipelined()
		parseData(pipeline)
		resetRedisConfig(jedis)
		println("\nAll data inserted in " + (System.nanoTime() - startTime) + " nanoseconds")
		jedis.close()
		jedisPooled.close()
	}

	def parseData(pipeline: Pipeline): Unit = {
		readLines("date.tbl", dateStructure, pipeline)
		readLines("customer.tbl", customerStructure, pipeline)
		readLines("supplier.tbl", supplierStructure, pipeline)
		readLines("part.tbl", partStructure, pipeline)
		readLines("lineorder.tbl", lineorderStructure, pipeline)
	}

	def readLines(fileName: String, dataStructure: Array[String], pipeline: Pipeline): Unit = {
		val tableName: String = fileName.split("\\.")(0);
		val startTime = System.nanoTime
		println("\nInserting data into " + tableName)
		Try(Source.fromFile(fileName)) match {
			case Success(file) =>
				file.getLines().grouped(1000).foreach(s => insertIntoRedis(s, tableName, dataStructure, tableName == "lineorder", pipeline))
				println("Inserted data into " + tableName + " in " + (System.nanoTime() - startTime) + " nanoseconds")
				file.close()
			case Failure(exception) =>
				println("Could not read file `" + fileName + "`. It should be placed into the same folder as the .jar")
				println(exception.getMessage)
		}
	}

	def insertIntoRedis(data: Seq[String], databaseName: String, dataStructure: Array[String], isLineOrder: Boolean, pipeline: Pipeline): Unit = {
		data.foreach(line => {
			val values = line.split("\\|")
			val valueIterator = values.iterator
			dataStructure.foreach(key => {
				if (valueIterator.hasNext) {
					if (isLineOrder) {
						pipeline.hset((databaseName + ":" + values(0) + ":" + values(1)).getBytes, key.getBytes, valueIterator.next().getBytes())
					} else {
						pipeline.hset((databaseName + ":" + values(0)).getBytes, key.getBytes, valueIterator.next().getBytes)
					}
				}
			})
		})
		pipeline.sync()
	}


}