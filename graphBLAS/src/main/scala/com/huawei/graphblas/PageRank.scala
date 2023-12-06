
package com.huawei.graphblas

import com.huawei.graphblas.Native
import com.huawei.graphblas.GraphBLASMatrix
import com.huawei.graphblas.PageRankResult
import com.huawei.graphblas.PageRankParameters


object PageRank {

	def runFromMatrix( matrix: GraphBLASMatrix, params: PageRankParameters ): PageRankResult = {
		val results = matrix.grbMatAddresses.map( ( matrix: Long ) => {
			if( matrix != 0L ) {
				Native.pagerankFromGrbMatrix( matrix, params.maxPageRankIteration,
					params.tolerance, params.numExperiments )
			} else 0L
		}
		).persist()
		new PageRankResult( matrix.grb, results, matrix.grb.getLastPageRankPerfStats() )
	}


	/**
	 * Performs a pagerank computation with GraphBLAS handling matrix input.
	 *
	 * @param[in,out] sc       The Spark context.
	 * @param[in,out] instance The GraphBLAS context.
	 * @param[in]     filename The absolute path to the matrix file. This file
	 *                         must exist on each worker node, and must have the
	 *                         exact same contents.
	 *
	 * @returns A handle to the distributed PageRank vector.
	 */
	def runFromFile( grb: GraphBLAS, filename: String, params: PageRankParameters ) : PageRankResult = {
		val fun = ( s: Int ) => {
			val ret = Native.pagerankFromFile( filename, params.maxPageRankIteration, params.tolerance, params.numExperiments )
			(s, ret)
		}
		val filter = ( x: (Int, Long) ) => x._1 != -1
		val arr = grb.runDistributedRdd( fun, (-1, 0L), filter, true )
		println( "Collected data instances per node:" )
		new PageRankResult( grb, arr.map( p => p._2 ), grb.getLastPageRankPerfStats() )
	}
}
