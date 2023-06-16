package de.aljoshavieth.redisssbinserter
package alternative_structures.discount_index

import redis.clients.jedis.{Jedis, JedisPooled, Pipeline}

import scala.io.Source
import scala.util.{Failure, Success, Try}

object InsertDiscountLineorderIndex extends Inserter {
	val lineorderStructure: Array[String] =
		Array("lo_orderkey", "lo_linenumber", "lo_custkey", "lo_partkey", "lo_suppkey", "lo_orderdate", "lo_orderpriority", "lo_shippriority", "lo_quantity", "lo_extendedprice", "lo_ordtotalprice", "lo_discount", "lo_revenue", "lo_supplycost", "lo_tax", "lo_commitdate", "lo_shipmod")


	override def execute(jedisPooled: JedisPooled, jedis: Jedis): Unit = {
		val startTime = System.nanoTime
		setRedisConfig(jedis)
		println("Parsing data...")
		val pipeline: Pipeline = jedis.pipelined()
		readLines("lineorder.tbl", lineorderStructure, pipeline)
		println("Inserting into Redis...")
		resetRedisConfig(jedis)
		println("\nAll data inserted in " + (System.nanoTime() - startTime) + " nanoseconds")
		jedis.close()
		//jedisPooled.close()

	}


	private def readLines(fileName: String, dataStructure: Array[String], pipeline: Pipeline): Unit = {
		val tableName: String = fileName.split("\\.")(0);
		val startTime = System.nanoTime
		println("\nInserting data into " + tableName)
		Try(Source.fromFile(fileName)) match {
			case Success(file) =>
				file.getLines().grouped(1000).foreach(s => insertIntoRedis(s, tableName, dataStructure, pipeline))
				println("Inserted data into " + tableName + " in " + (System.nanoTime() - startTime) + " nanoseconds")
				file.close()
			case Failure(exception) =>
				println("Could not read file `" + fileName + "`. It should be placed into the same folder as the .jar")
				println(exception.getMessage)
		}
	}

	private def insertIntoRedis(data: Seq[String], databaseName: String, dataStructure: Array[String], pipeline: Pipeline): Unit = {
		data.foreach(line => {
			val values = line.split("\\|")
			val valueIterator = values.iterator
			dataStructure.foreach(key => {
				if (valueIterator.hasNext) {
					pipeline.hset((databaseName + ":discount:" + values(11) + ":" + values(0) + ":" + values(1)).getBytes, key.getBytes, valueIterator.next().getBytes())
				}
			})
		})
		pipeline.sync()
	}


}
