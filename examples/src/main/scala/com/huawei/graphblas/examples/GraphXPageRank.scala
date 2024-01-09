
/*
 *   Copyright 2021 Huawei Technologies Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.huawei.graphblas.examples

import scala.math.sqrt

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import com.huawei.MatrixMarketReader
import com.huawei.graphblas.examples.cmdargs.PartitionedPageRankArgs
import com.huawei.graphblas.PageRankPerfStats
import com.huawei.graphblas.PageRankParameters

object GraphXPageRank {

	def benchmark( sc: SparkContext, file: String, params: PageRankParameters, normalized: Boolean ): PageRankPerfStats = {
		val times: Array[Double] = new Array[Double]( params.numExperiments )
		val iterations: Array[Int] = new Array[Int]( params.numExperiments )
		val matrix = MatrixMarketReader.readMM( sc, file )
		matrix.printSummary()
		val graphXmat = MatrixMarketReader.matrix2GraphX( sc, matrix )
		println( "GraphX: #edges is " + graphXmat.numEdges + ", #vertices is " + graphXmat.numVertices )
		var i: Int = 0
		while( i < params.numExperiments ) {
			val time: Long = System.nanoTime()
			// var checksum: (Long,Double) = (0,0)
			// the below two variants are within stddev distance from one another when run on gyro_m
			//val pr = graphXmat.pageRank( 0.0000001, 0.15 )
			//val pr = org.apache.spark.graphx.lib.PageRank.runUntilConvergence( graphXmat, 0.0000001, 0.15 )
			// the below variant does do something quite different
			val pr = org.apache.spark.graphx.lib.PageRank.runWithOptions( graphXmat, params.maxPageRankIterations, 0.15, None, normalized )
			// checksum = (pr.vertices.count(), pr.vertices.map( x => x._2 ).max())
			val checksum = ( pr.vertices.count(), pr.edges.count() )
			val time_taken: Double = (System.nanoTime() - time) / 1000000.0
			times( i ) = time_taken
			iterations( i ) = params.maxPageRankIterations
			println( s"Experiment ${i}: ${time_taken} seconds. Checksum: ${checksum}" )
			i += 1
		}
		new PageRankPerfStats( times.toIndexedSeq, iterations.toIndexedSeq )
    }

	def run( args: Array[String], normalize: Boolean ): Unit = {
		val prargs = PartitionedPageRankArgs.parseArguments( args )

		val sconf = new SparkConf().setAppName( "PageRank benchmarks" );
		val sc = new SparkContext( sconf );
		sc.setCheckpointDir( prargs.persistenceDirectory );
		prargs.forEachInputFile( x => {
			println( s"Starting benchmark using ${x}" )
			val results = benchmark( sc, x, prargs.makePageRankParameters(), normalize )
			print( s"Input: ${x} -- " )
			results.printStats()
		} )
    }

	def main( args: Array[String] ): Unit = {
		run( args, false )
	}
}
