
package com.huawei.graphblas.examples.cmdargs

import com.huawei.graphblas.examples.cmdargs.PageRankArgs

class GraphXPageRank( arguments: Seq[String] ) extends PageRankArgs( arguments ) {
	val numPartitions = opt[Int]( required = true )
	val persistenceDirectory = opt[String]( required = true )
}
