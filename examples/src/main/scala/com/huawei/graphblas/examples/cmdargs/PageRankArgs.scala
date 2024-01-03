
package com.huawei.graphblas.examples.cmdargs

import java.io.File
import java.io.FileNotFoundException

import org.rogach.scallop._

import com.huawei.graphblas.PageRankParameters

class PageRankArgs( arguments: Seq[String] ) extends ScallopConf( arguments ) {

	this.noshort = true

	// val inputFile = opt[String]( required = true, descr = "file with the input graph" )
	val inputFiles = trailArg[List[String]]( required = true )
	val numExperiments = opt[Int]( default = Option( 5 ), descr = "number of experiments, i.e., runs of PageRank" )
	val maxPageRankIterations = opt[Int]( default = Option( 50 ), descr = "maximum number of iterations for the PageRank algorithm" )
	val tolerance = opt[Double]( default = Option( Double.MinPositiveValue ), descr = "tolerance for the convergence of PageRank" )
	val verboseLog = opt[Boolean]()
	verify()

	def makePageRankParameters() : PageRankParameters = {
		return new PageRankParameters( maxPageRankIterations(), tolerance(), numExperiments() )
	}

	def getFilesCount() : Int = inputFiles().length

	def getInputFilePath() : String = {
		val filesNum = getFilesCount()
		if( filesNum != 1 ){
			throw new IllegalArgumentException( "multiple files available, please specify file number" )
		}
		getInputFilePath( 0 )
	}

	def getInputFilePath( num: Int ) : String = {
		val filesList = inputFiles()
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
