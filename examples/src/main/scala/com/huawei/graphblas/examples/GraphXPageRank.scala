
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

import com.huawei.Utils

object GraphXPageRank {

	def benchmark( sc: SparkContext, file: String, iters: Int, corrected: Boolean ): Unit = {
                val outerIt: Int = 5
		val times: Array[Double] = new Array[Double]( outerIt )
                val matrix = Utils.readMM( sc, file )
                println( "Matrix is " + matrix._1 + " by " + matrix._2 + " and has " + matrix._3 + " nonzeroes." );
                println( "Checksum (matrix rows): " + matrix._4.count() );
                val graphXmat = Utils.matrix2GraphX( sc, matrix )
                println( "GraphX: #edges is " + graphXmat.numEdges + ", #vertices is " + graphXmat.numVertices );
		for( i <- 1 to times.size ) {
			val time: Long = System.nanoTime()
                        var checksum: (Long,Double) = (0,0)
                        // the below two variants are within stddev distance from one another when run on gyro_m
                        //val pr = graphXmat.pageRank( 0.0000001, 0.15 )
                        //val pr = org.apache.spark.graphx.lib.PageRank.runUntilConvergence( graphXmat, 0.0000001, 0.15 )
                        // the below variant does do something quite different
                        val pr = org.apache.spark.graphx.lib.PageRank.runWithOptions( graphXmat, iters, 0.15, None, corrected )
                        checksum = (pr.vertices.count(), pr.vertices.map( x => x._2 ).max())
			val time_taken: Double = (System.nanoTime() - time) / 1000000000.0
			times(i-1) = time_taken
			println( s" Experiment $i: $time_taken seconds. Checksum: $checksum" )
                }
		val avg_time: Double = times.sum / times.size
		val sstddev:  Double = sqrt( times.map( x => (x - avg_time) * (x - avg_time) ).sum / (times.size-1) )
		println( s"Number of runs: ${times.size}.\nAverage time taken: $avg_time seconds.\nSample standard deviation: $sstddev" )
        }

	def main( args: Array[String] ): Unit = {
		val sconf = new SparkConf().setAppName( "PageRank benchmarks" );
		val sc = new SparkContext( sconf );
		val chkptdir = args(0);
                val iters: Int = args(1).toInt;
                val corrected: Boolean = args(2).toBoolean;
		sc.setCheckpointDir( chkptdir );
		val datafiles = args.drop(3);
		datafiles.foreach( x => {
			println( s"Starting benchmark using $x" );
			benchmark( sc, x, iters, corrected );
		} );
        }
}

