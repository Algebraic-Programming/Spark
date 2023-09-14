package com.huawei

import org.apache.spark.Partitioner

final case class CyclicPartitioner( numParts: Int ) extends Partitioner {

	def getPartition( key: Any ): Int = {
		val k: Int = key match {
			case a: Int => a
			case a: Long => a.toInt
			case _ => throw new RuntimeException( "unknown type" )
		}
		return k % numParts;
	}

	def numPartitions: Int = numParts;
}
