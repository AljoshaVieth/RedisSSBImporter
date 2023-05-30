package de.aljoshavieth.redisssbinserter

import datastructure.*

import redis.clients.jedis.{Jedis, JedisPooled, Pipeline}

import scala.io.Source
import scala.util.{Failure, Success, Try}

object InsertWithAdaptedStructure extends Inserter {



	var dateMap: Map[String, DateObject] = Map.empty[String, DateObject]
	var customerMap: Map[String, CustomerObject] = Map.empty[String, CustomerObject]
	var supplierMap: Map[String, SupplierObject] = Map.empty[String, SupplierObject]
	var partMap: Map[String, PartObject] = Map.empty[String, PartObject]
	var lineorderMap: Map[String, LineorderObject] = Map.empty[String, LineorderObject]


	override def execute(jedisPooled: JedisPooled, jedis: Jedis): Unit = {
		jedis.flushAll()
		val startTime = System.nanoTime
		setRedisConfig(jedis)
		println("Inserting data...")
		parseData()

		println("DateObjects: " + dateMap.size)
		println("CustomerObjects: " + customerMap.size)
		println("SupplierObjects: " + supplierMap.size)
		println("PartObjects: " + partMap.size)
		println("LineorderObjects: " + lineorderMap.size)



		val pipeline: Pipeline = jedisPooled.pipelined()
		insertLineorderFactsIntoRedis(pipeline)
		insertHelperTablesIntoRedis(pipeline)


		resetRedisConfig(jedis)
		println("\nAll data inserted in " + (System.nanoTime() - startTime) + " nanoseconds")
		jedis.close()
		jedisPooled.close()
	}


	def parseData(): Unit = {
		readLines("customer.tbl", createCustomerMap)
		readLines("date.tbl", createDateMap)
		readLines("supplier.tbl", createSupplierMap)
		readLines("part.tbl", createPartMap)
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

	def createPartMap(partLines:  Iterator[String]): Unit = {
		partMap =
			partLines.map(dateLine => {
				val attributes = dateLine.split('|')
				val part = PartObject(
					attributes(0),
					attributes(1),
					attributes(2),
					attributes(3),
					attributes(4),
					attributes(5),
					attributes(6),
					attributes(7),
					attributes(8)
				)
				(part.p_partkey, part)
			}).toMap
	}

	def createSupplierMap(supplierLines: Iterator[String]): Unit = {
		supplierMap =
			supplierLines.map(dateLine => {
				val attributes = dateLine.split('|')
				val supplier = SupplierObject(
					attributes(0),
					attributes(1),
					attributes(2),
					attributes(3),
					attributes(4),
					attributes(5),
					attributes(6)
				)
				(supplier.s_suppkey, supplier)
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

	def createCustomerMap(customerLines: Iterator[String]): Unit = {
		customerMap =
			customerLines.map(customerLine => {
				val attributes = customerLine.split('|')
				val customer = CustomerObject(
					attributes(0),
					attributes(1),
					attributes(2),
					attributes(3),
					attributes(4),
					attributes(5),
					attributes(6),
					attributes(7)
				)
				(customer.c_custkey, customer)
			}).toMap
	}

	/**
	 * Insert Sorted Sets
	 * Key is fact:d_year:d_monthnuminyear:d_daynuminmonth
	 * Values are Strings with this structure: lo_orderkey:lo_linenumber
	 * The Score of these Strings is the fact, e.g. lo_extendedprice or lo_discount
	 */
	def insertLineorderFactsIntoRedis(pipeline: Pipeline) = {
		println("Inserting lineorder facts to redis...")
		lineorderMap.values.foreach(lineorderObject => {
			val matchingDate: Option[DateObject] = dateMap.get(lineorderObject.lo_orderdate)
			matchingDate match {
				case Some(date) =>
					val extendedPriceKey = "lo_extendedprice:" + date.d_year + ":" + date.d_monthnuminyear + ":" + date.d_daynuminmonth
					pipeline.zadd(extendedPriceKey, lineorderObject.lo_extendedprice.toLong, lineorderObject.lo_orderkey + ":" + lineorderObject.lo_linenumber)

					val discountKey = "lo_discount:" + date.d_year + ":" + date.d_monthnuminyear + ":" + date.d_daynuminmonth
					pipeline.zadd(discountKey, lineorderObject.lo_discount.toLong, lineorderObject.lo_orderkey + ":" + lineorderObject.lo_linenumber)

					// This is pre-calculation...
					val discountedPriceKey = "lo_discountedprice:" + date.d_year + ":" + date.d_monthnuminyear + ":" + date.d_daynuminmonth
					pipeline.zadd(discountedPriceKey, lineorderObject.lo_discount.toLong * lineorderObject.lo_extendedprice.toLong, lineorderObject.lo_orderkey + ":" + lineorderObject.lo_linenumber)

				case None =>
				// ignore this object
			}
		})
		pipeline.sync()
		println("Insertion complete!")

	}


	def insertHelperTablesIntoRedis(pipeline: Pipeline) = {
		println("Inserting helper tables to redis...")
		lineorderMap.values.foreach(lineorderObject => {
			if (lineorderObject.lo_discount.toInt >= 1 && lineorderObject.lo_discount.toInt <= 3) {
				val discountBetween1And3Key = "lo_discount:between1And3"
				pipeline.zadd(discountBetween1And3Key, 0, lineorderObject.lo_orderkey + ":" + lineorderObject.lo_linenumber)
			}
			if (lineorderObject.lo_quantity.toLong < 25){
				val quantityLowerThan25 = "lo_quantity:lowerThan25"
				pipeline.zadd(quantityLowerThan25, 0, lineorderObject.lo_orderkey + ":" + lineorderObject.lo_linenumber)
			}

		})
		pipeline.sync()
		println("Insertion complete!")

	}

	def insertOnlyValidLineorderFactsIntoRedis(pipeline: Pipeline) = {
		println("Inserting lineorder facts to redis...")
		lineorderMap.values.foreach(lineorderObject => {
			val matchingDate: Option[DateObject] = dateMap.get(lineorderObject.lo_orderdate)
			matchingDate match {
				case Some(date) =>
					if(lineorderObject.lo_discount.toInt >= 1 && lineorderObject.lo_discount.toInt <= 3 && lineorderObject.lo_quantity.toLong < 25){
						// This is pre-calculation...
						val discountedPriceKey = "lo_discountedprice:discountBetween1And3+quantityLowerThan25" + date.d_year + ":" + date.d_monthnuminyear + ":" + date.d_daynuminmonth
						pipeline.zadd(discountedPriceKey, lineorderObject.lo_discount.toLong * lineorderObject.lo_extendedprice.toLong, lineorderObject.lo_orderkey + ":" + lineorderObject.lo_linenumber)
					}

				case None =>
				// ignore this object
			}
		})
		pipeline.sync()
		println("Insertion complete!")

	}



}