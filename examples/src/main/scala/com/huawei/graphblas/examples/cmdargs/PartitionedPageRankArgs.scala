
package com.huawei.graphblas.examples.cmdargs

import com.huawei.graphblas.examples.cmdargs.PageRankArgs

class PartitionedPageRankArgs( arguments: Seq[String] ) extends PageRankArgs( arguments ) {
	val numPartitions = opt[Int]( required = true )
	val persistenceDirectory = opt[String]( required = true )
}
