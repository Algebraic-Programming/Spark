
package com.huawei.graphblas

import scala.collection.immutable.IndexedSeq
import com.huawei.graphblas.PerfStats
import java.lang.IllegalArgumentException


final class PageRankPerfStats( times: IndexedSeq[ Double ], val numIterations: IndexedSeq[ Int ] )
	extends PerfStats( times) {
	if( times.length != numIterations.length ){
		throw new IllegalArgumentException( "lengths differ" )
	}

	def sameNumIteration: Boolean = {
		// numIterarions.min != numIterarions.max
		numIterations.forall( _ == numIterations( 0 ) )
	}

	def iterations: Int = {
		if( sameNumIteration ) {
			numIterations( 0 )
		} else {
			throw new RuntimeException( "number of iterations differ" )
		}
	}

	override def printStats(): Unit = {
		if( sameNumIteration ) {
			print( s"iterations across all tests: ${numIterations(0)}; " )
		} else {
			print( "iterations: [" )
			numIterations.foreach( i => print( s" ${i}" ) )
			print( " ]; " )
		}
		super.printStats()
	}
}
