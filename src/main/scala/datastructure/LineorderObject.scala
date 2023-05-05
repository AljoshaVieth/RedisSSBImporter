package de.aljoshavieth.redisssbinserter
package datastructure

case class LineorderObject(lo_orderkey: String,
						   lo_linenumber: String,
						   lo_custkey: String,
						   lo_partkey: String,
						   lo_suppkey: String,
						   lo_orderdate: String,
						   lo_orderpriority: String,
						   lo_shippriority: String,
						   lo_quantity: String,
						   lo_extendedprice: String,
						   lo_ordtotalprice: String,
						   lo_discount: String,
						   lo_revenue: String,
						   lo_supplycost: String,
						   lo_tax: String,
						   lo_commitdate: String,
						   lo_shipmod: String
					)
