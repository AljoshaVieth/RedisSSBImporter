package de.aljoshavieth.redisssbinserter

import redis.clients.jedis.{Jedis, JedisPooled}

object main {
	def main(args: Array[String]): Unit = {
		// Using a single Jedis instance to set config an flushAll because JedisPooled does not have this methods.
		val jedis = new Jedis("localhost", 6380)
		val jedisPooled = new JedisPooled("localhost", 6380)
		InsertWithAdaptedStructure.execute(jedisPooled, jedis)
		
		/*
		// Using a single Jedis instance to set config an flushAll because JedisPooled does not have this methods.
		val jedis = new Jedis("localhost", 6379)
		val jedisPooled = new JedisPooled("localhost", 6379)
		*/
	}
}
