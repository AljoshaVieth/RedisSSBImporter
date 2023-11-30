package de.aljoshavieth.redisssbinserter

import normalizedStructure.InsertWithNormalizedStructure
import de.aljoshavieth.redisssbinserter.abandoned.alternative_structures.date_lineorderkey_index.InsertDateLineorderkeyIndex
import de.aljoshavieth.redisssbinserter.abandoned.alternative_structures.discount_index.InsertDiscountLineorderIndex
import de.aljoshavieth.redisssbinserter.abandoned.alternative_structures.orderdate_lineorder.InsertOrderdateLineorderIndex
import de.aljoshavieth.redisssbinserter.abandoned.alternative_structures.year_lineorder_index.InsertYearLineorderIndex
import de.aljoshavieth.redisssbinserter.denormalized.InsertWithDenormalizedStructure
import redis.clients.jedis.{Jedis, JedisPooled}

object Main {
	def main(args: Array[String]): Unit = {
		// Using a single Jedis instance to set config and flushAll because JedisPooled does not have this methods.
		val jedis = new Jedis("localhost", 6379)
		val jedisPooled = new JedisPooled("localhost", 6379)
		jedis.getClient.setTimeoutInfinite()
		//InsertWithClassicStructure.execute(jedisPooled, jedis)
		InsertWithDenormalizedStructure.execute(jedisPooled, jedis)
		// Alternative, abandoned approaches:
		//InsertDateLineorderkeyIndex.execute(null, jedis)
		//InsertOrderdateLineorderIndex.execute(null, jedis)
		//InsertDiscountLineorderIndex.execute(null, jedis)
		//InsertWithAdaptedStructure.execute(jedisPooled, jedis)
	}
}
