package de.aljoshavieth.redisssbinserter

import classicStructure.InsertWithClassicStructure

import de.aljoshavieth.redisssbinserter.alternative_structures.discount_index.InsertDiscountLineorderIndex
import de.aljoshavieth.redisssbinserter.alternative_structures.orderdate_lineorder.InsertOrderdateLineorderIndex
import de.aljoshavieth.redisssbinserter.alternative_structures.year_lineorder_index.InsertYearLineorderIndex
import redis.clients.jedis.{Jedis, JedisPooled}

object Main {
	def main(args: Array[String]): Unit = {
		// Using a single Jedis instance to set config an flushAll because JedisPooled does not have this methods.
		val jedis = new Jedis("localhost", 6379)
		jedis.select(1) // Set the database index
		InsertOrderdateLineorderIndex.execute(null, jedis)
		//val jedisPooled = new JedisPooled("localhost", 6379)
		//InsertDiscountLineorderIndex.execute(null, jedis)
		//InsertWithAdaptedStructure.execute(jedisPooled, jedis)
		//InsertWithClassicStructure.execute(jedisPooled, jedis)
		//InsertYearLineorderIndex.execute(jedisPooled, jedis)

		/*
		// Using a single Jedis instance to set config an flushAll because JedisPooled does not have this methods.
		val jedis = new Jedis("localhost", 6379)
		val jedisPooled = new JedisPooled("localhost", 6379)
		*/
	}
}
