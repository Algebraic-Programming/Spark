
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

object GraphXPageRank {

	def benchmark( sc: SparkContext, file: String ): Unit = {
                val outerIt: Int = 5
		val times: Array[Double] = new Array[Double]( outerIt )
		for( i <- 1 to times.size ) {
			val time: Long = System.nanoTime()
                        //graphX call goes here
                        val checksum: Int = 0 // the number of output elements goes here
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
		sc.setCheckpointDir( chkptdir );
		val datafiles = args.drop(1);
		datafiles.foreach( x => {
			println( s"Starting benchmark using $x" );
			benchmark( sc, x );
		} );
        }
}

