
/*
 *   Copyright 2023 Huawei Technologies Co., Ltd.
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

import java.io.File
import scala.math.abs
import scala.math.max
import scala.math.sqrt

import com.huawei.Utils

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import com.huawei.graphblas.examples.GraphMatrix

object SimpleSparkPagerank {

	def spark_pr( matrix: RDD[ (Long, Iterable[Long] ) ], iters: Int, cp_freq: Int ): RDD[ (Long, Double) ] = {
		var ranks: RDD[ (Long, Double) ] = matrix.map( x => (x._1, 1.0 ) )
		var i: Int = 1
		while( i <= iters ) {
			if( (i % (cp_freq+1)) == cp_freq ) {
				ranks.persist()
			}
			ranks = matrix.join(ranks)
				.flatMap( el => {
					val x = el._2
					x._1.map( y => (y, 0.15 + 0.85 * x._2) )
				}
			).reduceByKey( _ + _ )
			i += 1
		}
		ranks.persist();
	}

	def benchmark( matrix: GraphMatrix, iters: Int, cp_freq: Int = 30, num_experiments: Int = 5 ): Unit = {

		val times: Array[Double] = new Array[Double]( num_experiments )

		val time = System.nanoTime()
		spark_pr(matrix.data, iters, cp_freq ).unpersist()
		val dry_t1 = (System.nanoTime() - time) / 1000000000.0
		println( s"Time taken for dry run: $dry_t1 seconds." )

		println( "Performing benchmark of the flop-variant:" )
		var i = 0
		while( i < times.size ) {
			val time: Long = System.nanoTime()
			val ranks = spark_pr(matrix.data, iters, cp_freq )
			val checksum = ranks.count()
			ranks.unpersist()
			val time_taken: Double = (System.nanoTime() - time) / 1000000000.0
			times(i) = time_taken
			println( s" Experiment $i: $time_taken seconds. Checksum: $checksum" )
			i += 1
		}
		val avg_time: Double = times.sum / times.size
		val sstddev:  Double = sqrt( times.map( x => (x - avg_time) * (x - avg_time) ).sum / (times.size-1) )
		println( s"Number of runs: ${times.size}." )
		println( s"Average time taken: ${avg_time} seconds." )
		println( s"Sample standard deviation: ${sstddev}" )
		println( s"Number of iterations: ${iters}" )
	}

	def main( args: Array[String] ): Unit = {
		if( args.length < 4 ) {
			println( "Mandatory argument #1: number of parts input should be divided into." );
			println( "Mandatory argument #2: checkpoint directory." );
			println( "Mandatory argument #3: maximum number of iterations." );
			println( "Mandatory argument #4: input matrix." );
			return;
		}

		val sconf = new SparkConf().setAppName( "Simple PageRank benchmark" );
		val sc = new SparkContext( sconf );

		val P = args(0).toInt;
		val chkptdir = new File( args(1) ).getCanonicalPath();
		val num_iterations = args(2).toInt;
		val infile: File = new File( args(3) );
		if( !infile.exists() || infile.isDirectory()) {
			println( s"cannot access file ${args(3)}, or is a directory" )
			return
		}
		val filePath = infile.getCanonicalPath()
		println( s"reading from file ${filePath}" )

		sc.setCheckpointDir( chkptdir );
		val matrix: GraphMatrix = GraphMatrix( sc, filePath, P, true )

		println( s"Pagerank example called with $P parts for matrix and vector segments." );
		println( s"Pagerank example called using $chkptdir as checkpoint directory." );

		benchmark( matrix, num_iterations )
	}

}