package de.aljoshavieth.redisssbinserter
package alternative_structures.denormalized

import datastructure.*
import datastructure.shortened.*

import redis.clients.jedis.{Jedis, JedisPooled, Pipeline}

import scala.io.Source
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

object InsertWithDenormalizedStructure extends Inserter {


	var customerMap: Map[String, ShortenedCustomerObject] = Map.empty[String, ShortenedCustomerObject]
	var supplierMap: Map[String, ShortenedSupplierObject] = Map.empty[String, ShortenedSupplierObject]
	var partMap: Map[String, ShortenedPartObject] = Map.empty[String, ShortenedPartObject]
	var dateMap: Map[String, ShortenedDateObject] = Map.empty[String, ShortenedDateObject]

	var lineorderMap: Map[String, ShortenedLineorderObject] = Map.empty[String, ShortenedLineorderObject]

	var denormalizedMap: Map[String, DenormalizedObject] = Map.empty[String, DenormalizedObject]


	override def execute(jedisPooled: JedisPooled, jedis: Jedis): Unit = {
		jedis.flushAll()
		val startTime = System.currentTimeMillis()
		setRedisConfig(jedis)
		println("Inserting data...")
		parseData()

		println("CustomerObjects: " + customerMap.size)
		println("SupplierObjects: " + supplierMap.size)
		println("PartObjects: " + partMap.size)
		println("DateObjects: " + dateMap.size)
		println("LineorderObjects: " + lineorderMap.size)

		println("Building denormalized map")

		createDenormalizedMap(customerMap, supplierMap, partMap, lineorderMap, dateMap)

		val pipeline: Pipeline = jedisPooled.pipelined()
		insertDenormalizedObjectsIntoRedis(pipeline)

		resetRedisConfig(jedis)
		println("\nAll data inserted in " + (System.currentTimeMillis() - startTime) + " ms")
		jedis.close()
		jedisPooled.close()
	}


	def parseData(): Unit = {
		readLines("customer.tbl", createCustomerMap)
		readLines("supplier.tbl", createSupplierMap)
		readLines("part.tbl", createPartMap)
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

	def createPartMap(partLines: Iterator[String]): Unit = {
		partMap =
			partLines.map(dateLine => {
				val attributes = dateLine.split('|')
				val part = ShortenedPartObject(
					attributes(0),
					attributes(2),
					attributes(3),
					attributes(4)
				)
				(part.p_partkey, part)
			}).toMap
	}

	def createDateMap(dateLines: Iterator[String]): Unit = {
		dateMap =
			dateLines.map(dateLine => {
				val attributes = dateLine.split('|')
				val date = ShortenedDateObject(
					attributes(0),
					attributes(4)
				)
				(date.d_datekey, date)
			}).toMap
	}

	def createSupplierMap(supplierLines: Iterator[String]): Unit = {
		supplierMap =
			supplierLines.map(dateLine => {
				val attributes = dateLine.split('|')
				val supplier = ShortenedSupplierObject(
					attributes(0),
					attributes(3),
					attributes(4),
					attributes(5)
				)
				(supplier.s_suppkey, supplier)
			}).toMap
	}

	def createLineorderMap(lineorderLines: Iterator[String]): Unit = {
		lineorderMap =
			lineorderLines.map(lineorderLine => {
				val attributes = lineorderLine.split('|')
				val lineorder = ShortenedLineorderObject(
					attributes(0),
					attributes(1),
					attributes(2),
					attributes(3),
					attributes(4),
					attributes(5),
					attributes(8),
					attributes(9),
					attributes(11),
					attributes(12),
					attributes(13)

				)
				(lineorder.lo_orderkey + ":" + lineorder.lo_linenumber, lineorder)
			}).toMap
	}

	def createCustomerMap(customerLines: Iterator[String]): Unit = {
		customerMap =
			customerLines.map(customerLine => {
				val attributes = customerLine.split('|')
				val customer = ShortenedCustomerObject(
					attributes(0),
					attributes(3),
					attributes(4),
					attributes(5)
				)
				(customer.c_custkey, customer)
			}).toMap
	}

	def createDenormalizedMap(

								 customerMap: Map[String, ShortenedCustomerObject],
								 supplierMap: Map[String, ShortenedSupplierObject],
								 partMap: Map[String, ShortenedPartObject],
								 lineorderMap: Map[String, ShortenedLineorderObject],
								 dateMap: Map[String, ShortenedDateObject]): Unit = {

		lineorderMap.foreach { case (key, lineorder) =>
			val customer = customerMap.getOrElse(lineorder.lo_custkey, ShortenedCustomerObject("", "", "", ""))
			val supplier = supplierMap.getOrElse(lineorder.lo_suppkey, ShortenedSupplierObject("", "", "", ""))
			val part = partMap.getOrElse(lineorder.lo_partkey, ShortenedPartObject("", "", "", ""))
			val date = dateMap.getOrElse(lineorder.lo_orderdate, ShortenedDateObject("", ""))

			val denormalized = DenormalizedObject(
				lineorder.lo_orderkey,
				lineorder.lo_linenumber,
				lineorder.lo_custkey,
				lineorder.lo_discount,
				lineorder.lo_extendedprice,
				lineorder.lo_orderdate,
				lineorder.lo_partkey,
				lineorder.lo_quantity,
				lineorder.lo_revenue,
				lineorder.lo_suppkey,
				lineorder.lo_supplycost,
				customer.c_city,
				customer.c_nation,
				part.p_category,
				supplier.s_city,
				supplier.s_nation,
				customer.c_region,
				supplier.s_region,
				part.p_brand1,
				date.d_year,
				part.p_mfgr
			)

			denormalizedMap += (key -> denormalized)
		}


	}


	var numOfInsertedRecords = 0

	private def insertDenormalizedObjectsIntoRedis(pipeline: Pipeline): Unit = {
		println("Inserting denormalized objects into redis...")
		println("NUmber of denormalized objects: " + denormalizedMap.size)
		denormalizedMap.values.grouped(1000).foreach(denormalizedObjects => {
			denormalizedObjects.foreach(denormalizedObject => {
				val hash = Map(
					"lo_orderkey" -> denormalizedObject.lo_orderkey,
					"lo_linenumber" -> denormalizedObject.lo_linenumber,
					"lo_custkey" -> denormalizedObject.lo_custkey,
					"lo_discount" -> denormalizedObject.lo_discount,
					"lo_extendedprice" -> denormalizedObject.lo_extendedprice,
					"lo_orderdate" -> denormalizedObject.lo_orderdate,
					"lo_partkey" -> denormalizedObject.lo_partkey,
					"lo_quantity" -> denormalizedObject.lo_quantity,
					"lo_revenue" -> denormalizedObject.lo_revenue,
					"lo_suppkey" -> denormalizedObject.lo_suppkey,
					"lo_supplycost" -> denormalizedObject.lo_supplycost,
					"c_city" -> denormalizedObject.c_city,
					"c_nation" -> denormalizedObject.c_nation,
					"p_category" -> denormalizedObject.p_category,
					"s_city" -> denormalizedObject.s_city,
					"s_nation" -> denormalizedObject.s_nation,
					"c_region" -> denormalizedObject.c_region,
					"s_region" -> denormalizedObject.s_region,
					"p_brand1" -> denormalizedObject.p_brand1,
					"d_year" -> denormalizedObject.d_year,
					"p_mfgr" -> denormalizedObject.p_mfgr
				)
				pipeline.hmset("lineorder:" + denormalizedObject.lo_orderkey + ":" + denormalizedObject.lo_linenumber, hash.asJava)
				numOfInsertedRecords = numOfInsertedRecords + 1
				println("Inserted Record " + numOfInsertedRecords + " : " + hash)
			})
			pipeline.sync()
		})

		println("Finished inserting denormalized objects into redis.")
	}


}