
package com.huawei.graphblas.examples.cmdargs

import java.io.File
import java.io.FileNotFoundException

import org.rogach.scallop._

import com.huawei.graphblas.PageRankParameters

class PageRankArgs( arguments: Seq[String] ) extends ScallopConf( arguments ) {

	this.noshort = true

	val inputFile = opt[String]( required = true, descr = "file with the input graph" )
	val numExperiments = opt[Int]( default = Option( 5 ), descr = "number of experiments, i.e., runs of PageRank" )
	val maxPageRankIteration = opt[Int]( default = Option( 50 ), descr = "maximum number of iterations for the PageRank algorithm" )
	val tolerance = opt[Double]( default = Option( Double.MinPositiveValue ), descr = "tolerance for the convergence of PageRank" )
	val verboseLog = opt[Boolean]()
	verify()

	def makePageRankParameters() : PageRankParameters = {
		return new PageRankParameters( maxPageRankIteration(), tolerance(), numExperiments() )
	}

	def getInputFilePath() : String = {
		val infile: File = new File( inputFile() );
		if( !infile.exists() || infile.isDirectory()) {
			throw new FileNotFoundException( s"cannot access file ${args(0)}, or is a directory" )
		}
		infile.getCanonicalPath()
	}
}
