package de.aljoshavieth.redisssbinserter
package abandoned.alternative_structures.year_lineorder_index

import datastructure.*

import redis.clients.jedis.{Jedis, JedisPooled, Pipeline}

import scala.io.Source
import scala.util.{Failure, Success, Try}

object InsertYearLineorderIndex extends Inserter {
	var dateMap: Map[String, DateObject] = Map.empty[String, DateObject]
	var lineorderMap: Map[String, LineorderObject] = Map.empty[String, LineorderObject]

	override def execute(jedisPooled: JedisPooled, jedis: Jedis): Unit = {
		val startTime = System.nanoTime
		setRedisConfig(jedis)
		println("Parsing data...")
		parseData()

		println("DateObjects: " + dateMap.size)
		println("LineorderObjects: " + lineorderMap.size)

		println("Starting linkLineordersToDates...")
		val yearLineorderMap = linkLineordersToYear(dateMap, lineorderMap.values.toList)
		println(yearLineorderMap.size)
		yearLineorderMap.values.foreach(x => println(x.size))
		val pipeline: Pipeline = jedis.pipelined()
		println("Inserting into Redis...")
		insertAsListIntoRedis(pipeline, yearLineorderMap)
		//val yearDatekeyMap = linkDateToYear(dateMap)
		//insertDatekeysAsQueryStrings(pipeline, yearDatekeyMap)

		resetRedisConfig(jedis)
		println("\nAll data inserted in " + (System.nanoTime() - startTime) + " nanoseconds")
		jedis.close()
		//jedisPooled.close()

	}


	def parseData(): Unit = {
		readLines("date.tbl", createDateMap)
		readLines("lineorder.tbl", createLineorderMap)
	}

	def readLines(fileName: String, f: Iterator[String] => Unit): Unit = {
		val tableName: String = fileName.split("\\.")(0);
		val startTime = System.nanoTime
		println("\nReading " + tableName + " file")
		Try(Source.fromFile(fileName)) match {
			case Success(file) =>
				f(file.getLines())
				println("Extracted data from " + tableName + " in " + (System.nanoTime() - startTime) + " nanoseconds")
				file.close()
			case Failure(exception) =>
				println("Could not read file `" + fileName + "`. It should be placed into the same folder as the .jar")
				println(exception.getMessage)
		}
	}

	def createDateMap(dateLines: Iterator[String]): Unit = {
		dateMap =
			dateLines.map(dateLine => {
				val attributes = dateLine.split('|')
				val date = DateObject(
					attributes(0),
					attributes(1),
					attributes(2),
					attributes(3),
					attributes(4),
					attributes(5),
					attributes(6),
					attributes(7),
					attributes(8),
					attributes(9),
					attributes(10),
					attributes(11),
					attributes(12),
					attributes(13),
					attributes(14),
					attributes(15),
					attributes(16)
				)
				(date.d_datekey, date)
			}).toMap
	}

	def createLineorderMap(lineorderLines: Iterator[String]): Unit = {
		lineorderMap =
			lineorderLines.map(lineorderLine => {
				val attributes = lineorderLine.split('|')
				val lineorder = LineorderObject(
					attributes(0),
					attributes(1),
					attributes(2),
					attributes(3),
					attributes(4),
					attributes(5),
					attributes(6),
					attributes(7),
					attributes(8),
					attributes(9),
					attributes(10),
					attributes(11),
					attributes(12),
					attributes(13),
					attributes(14),
					attributes(15),
					attributes(16)
				)
				(lineorder.lo_orderkey + ":" + lineorder.lo_linenumber, lineorder)
			}).toMap
	}

	private def linkLineordersToYear(
										dateMap: Map[String, DateObject],
										lineorderObjects: List[LineorderObject]): Map[String, List[String]] = {
		lineorderObjects.flatMap { lineorderObject =>
			dateMap.get(lineorderObject.lo_orderdate).map { dateObject =>
				// Check if the order date exists in the dateMap and map it to a result if present
				val year = dateObject.d_year
				val lineorderKey = lineorderObject.lo_orderkey + ":" + lineorderObject.lo_linenumber
				(year, lineorderKey) // Return a tuple of year and lineorderKey
			}
		}.groupMap(_._1)(_._2) // Group the tuples by year and collect lineorderKeys into a list for each year
	}

	private def insertAsListIntoRedis(pipeline: Pipeline, map: Map[String, List[String]]): Unit = {
		map.foreach((k, v) => {
			println("key: " + "yearLineorderIndex:" + k)
			pipeline.rpush("yearLineorderIndex:" + k, v: _*)
		})
		pipeline.sync()
		println("Everything Inserted")
	}

	private def linkDateToYear(dateMap: Map[String, DateObject]): Map[String, List[String]] = {
		dateMap
			.groupBy { case (_, dateObj) => dateObj.d_year }
			.view.mapValues(_.values.map(_.d_datekey).toList).toMap
	}

	
	private def insertDatekeysAsQueryStrings(pipeline: Pipeline, map: Map[String, List[String]]): Unit = {
		map.foreach((k, v) => {
			pipeline.set("yearDateIndex:" + k, v.map(str => s"@lo_orderdate:[$str $str]").mkString(" | "))
		})
		pipeline.sync()
		println("Everything Inserted")
	}
	


}
