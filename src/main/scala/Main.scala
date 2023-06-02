package de.aljoshavieth.redisssbinserter

import classicStructure.{InsertWithClassicStructure, InsertYearLineorderIndex}

import redis.clients.jedis.{Jedis, JedisPooled}

object Main {
	def main(args: Array[String]): Unit = {
		// Using a single Jedis instance to set config an flushAll because JedisPooled does not have this methods.
		val jedis = new Jedis("localhost", 6379)
		val jedisPooled = new JedisPooled("localhost", 6379)
		//InsertWithAdaptedStructure.execute(jedisPooled, jedis)
		//InsertWithClassicStructure.execute(jedisPooled, jedis)
		InsertYearLineorderIndex.execute(jedisPooled, jedis)

		/*
		// Using a single Jedis instance to set config an flushAll because JedisPooled does not have this methods.
		val jedis = new Jedis("localhost", 6379)
		val jedisPooled = new JedisPooled("localhost", 6379)
		*/
	}
}
