
package com.huawei.graphblas.examples.cmdargs

import java.io.File
import java.io.FileNotFoundException

class PageRankInput {

	var inputFiles: List[String] = List[String]()

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
