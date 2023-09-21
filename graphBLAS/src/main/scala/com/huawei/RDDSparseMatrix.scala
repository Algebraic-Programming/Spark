package com.huawei

import org.apache.spark.rdd.RDD

class RDDSparseMatrix(
	val m: Long,
	val n: Long,
	val nnz: Long,
	val data: RDD[ (Long, Iterable[Long], Iterable[Double]) ])
{
	def printSummary(): Unit = {
		println( "Matrix is " + m + " by " + n + " and has " + nnz + " nonzeroes." )
		println( "Checksum (matrix rows): " + data.count() )
	}
}
