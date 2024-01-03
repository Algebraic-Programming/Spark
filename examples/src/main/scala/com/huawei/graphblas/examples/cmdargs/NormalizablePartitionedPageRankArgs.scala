
package com.huawei.graphblas.examples.cmdargs

import com.huawei.graphblas.examples.cmdargs.PartitionedPageRankArgs

class NormalizablePartitionedPageRankArgs( arguments: Seq[String] ) extends PartitionedPageRankArgs( arguments ) {
	val normalize = opt[Boolean]()
}
