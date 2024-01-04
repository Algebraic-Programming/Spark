
package com.huawei.graphblas.examples.cmdargs

import scopt.OParser

import com.huawei.graphblas.examples.cmdargs.PageRankArgs

class PartitionedPageRankArgs extends PageRankArgs {
	var numPartitions: Int = 0
	var persistenceDirectory: String = ""
}


object PartitionedPageRankArgs extends PageRankParser[ PartitionedPageRankArgs ] {

	def makeDefaultObject(): PartitionedPageRankArgs = new PartitionedPageRankArgs()

	def makeParser[ T <: PartitionedPageRankArgs ](): OParser[Unit, T] = {

		val preBuilder = PageRankArgs.makeParser[ T ]()
		val builder = OParser.builder[ T ]
		val parser1 = {
			import builder._
			OParser.sequence(
				opt[Int]("num-partitions").required()
					.action((x, c) => {c.numPartitions = x; c})
					.text("number of partitions to split the input graph"),
				opt[String]("persistence-directory").required()
					.action((x, c) => {c.persistenceDirectory = x; c})
					.text("directory to persists intermediate results for PageRank iterations")
			)
		}
		preBuilder ++ parser1
	}
}
