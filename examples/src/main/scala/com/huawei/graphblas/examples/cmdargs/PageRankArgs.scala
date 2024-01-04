
package com.huawei.graphblas.examples.cmdargs

import java.io.File
import java.io.FileNotFoundException

import scopt.OParser

import com.huawei.graphblas.PageRankParameters

class PageRankArgs {

	var numExperiments: Int = 5
	var maxPageRankIterations: Int = 50
	var tolerance: Double = Double.MinPositiveValue
	var verboseLog: Boolean = false
	var inputFiles: List[String] = List[String]()


	def makePageRankParameters() : PageRankParameters = {
		return new PageRankParameters( maxPageRankIterations, tolerance, numExperiments )
	}

	def getFilesCount() : Int = inputFiles.length

	def getInputFilePath() : String = {
		val filesNum = getFilesCount()
		if( filesNum != 1 ){
			throw new IllegalArgumentException( "multiple files available, please specify file number" )
		}
		getInputFilePath( 0 )
	}

	def getInputFilePath( num: Int ) : String = {
		val filesList = inputFiles
		if( num >= filesList.length ) {
			throw new ArrayIndexOutOfBoundsException( "Index " + num +
				" is not valid: only " + filesList.length + " available" )
		}
		val infile: File = new File( filesList( num ) );
		if( !infile.exists() || infile.isDirectory()) {
			throw new FileNotFoundException( s"cannot access file ${infile}, or is a directory" )
		}
		infile.getCanonicalPath()
	}

	def forEachInputFile( f: (String) => Unit ): Unit = {
		val numFiles = getFilesCount()
		var i = 0
		while( i < numFiles ) {
			val file = getInputFilePath( i )
			f( file )
			i += 1
		}
	}
}

object PageRankArgs extends PageRankParser[ PageRankArgs ] {

	def makeDefaultObject(): PageRankArgs = new PageRankArgs()

	def makeParser[ T <: PageRankArgs ](): OParser[ Unit, T ] = {
		val builder = OParser.builder[ T ]
		val parser1 = {
			import builder._
			OParser.sequence(
				programName("<program>"),
				// head("scopt", "4.x"),
				help('h', "help")
					.text("print this help"),
				opt[Int]("num-experiments").optional()
					.action((x, c) => {c.numExperiments = x; c})
					.text("number of experiments to perform"),
				opt[Int]("max-page-rank-iterations").optional()
					.action((x, c) => {c.maxPageRankIterations = x; c})
					.text("maximum number of PageRank iterations"),
				opt[Double]("tolerance").optional()
					.action((x, c) => {c.tolerance = x; c})
					.text("tolerance for the PageRank"),
				opt[Unit]("verbose-log").optional()
					.action((_, c) => {c.verboseLog = true; c})
					.text("set verbose logging"),
				help("help").text("prints this usage text"),
				arg[String]("<input file>...")
					.minOccurs(1)
					.required()
					.action((x, c) => {c.inputFiles = c.inputFiles :+ x; c})
					.text("optional unbounded args")
			)
		}
		parser1
	}
}
