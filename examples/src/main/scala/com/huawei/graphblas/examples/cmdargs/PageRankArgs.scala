
package com.huawei.graphblas.examples.cmdargs

import scopt.{ OParser, OptionDef }

import com.huawei.graphblas.PageRankParameters

class PageRankArgs extends PageRankInput {

	var numExperiments: Int = PageRankArgs.numExperimentsDefault
	var maxPageRankIterations: Int = PageRankArgs.maxPageRankIterationsDefault
	var tolerance: Double = PageRankArgs.toleranceDefault
	var verboseLog: Boolean = PageRankArgs.verboseLogDefault

	def makePageRankParameters() : PageRankParameters = {
		return new PageRankParameters( maxPageRankIterations, tolerance, numExperiments )
	}
}

object PageRankArgs extends PageRankParser[ PageRankArgs ] {

	val numExperimentsDefault: Int = 5
	val maxPageRankIterationsDefault: Int = 55
	val toleranceDefault: Double = Double.MinPositiveValue
	val verboseLogDefault: Boolean = false

	def makeDefaultObject(): PageRankArgs = new PageRankArgs()

	def makeParser[ T <: PageRankArgs ](): List[OptionDef[_, T]] = {
		val builder = OParser.builder[ T ]
		val parser1 = {
			import builder._
			OParser.sequence(
				opt[Int]("num-experiments").optional()
					.action((x, c) => {c.numExperiments = x; c})
					.text( s"number of experiments to perform; default: ${numExperimentsDefault}" ),
				opt[Int]("max-page-rank-iterations").optional()
					.action((x, c) => {c.maxPageRankIterations = x; c})
					.text( s"maximum number of PageRank iterations; default: ${maxPageRankIterationsDefault}" ),
				opt[Double]("tolerance").optional()
					.action((x, c) => {c.tolerance = x; c})
					.text( s"tolerance for the PageRank; default: ${toleranceDefault}" ),
				opt[Unit]("verbose-log").optional()
					.action((_, c) => {c.verboseLog = true; c})
					.text( s"set verbose logging; default: ${verboseLogDefault}" )
			)
		}
		parser1.toList
	}
}
