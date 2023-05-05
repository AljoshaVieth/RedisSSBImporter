package de.aljoshavieth.redisssbinserter

import redis.clients.jedis.{Jedis, JedisPooled}

abstract class Inserter {
	def execute(jedisPooled: JedisPooled, jedis: Jedis): Unit

	def setRedisConfig(jedis: Jedis): Unit = {
		jedis.configSet("appendonly", "no")
		jedis.configSet("save", "")
	}

	def resetRedisConfig(jedis: Jedis): Unit = {
		jedis.configSet("appendonly", "yes")
		jedis.configSet("save", "3600 1") // perform snapshot every hour if at least one key has been changed
	}
}
