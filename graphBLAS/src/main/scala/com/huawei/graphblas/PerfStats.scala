
package com.huawei.graphblas

import scala.collection.immutable.IndexedSeq
import java.lang.IllegalArgumentException
import com.huawei.Utils

class PerfStats( val times: IndexedSeq[ Double ] ) {
	if( times.length == 0 ){
		throw new IllegalArgumentException( "empty array" )
	}

	def numExperiments: Int = times.length
	
	def timeMean: Double = Utils.computeMean( times )

	def timeStdDeviation: Double = Utils.computeStdDev( times )

	def printStats(): Unit = {
		print( s"number of experiments: ${numExperiments}; ")
		val mean = timeMean
		print( s"mean duration (ms): ${timeMean}; " )
		if( numExperiments > 1 ) {
			println( s"std duration (ms): ${timeStdDeviation}" )
		} else {
			println( "no deviation" )
		}
	}
}
