
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

import java.io.File
import org.apache.spark.SparkEnv
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.util.Using

import com.huawei.Utils
import com.huawei.graphblas.GraphBLAS
import com.huawei.graphblas.PageRank
import com.huawei.graphblas.PageRankResult


object Pagerank {

	def spark_pr( matrix: org.apache.spark.rdd.RDD[ (Int, Iterable[Int] ) ], iters: Int, cp_freq: Int = 30 ): org.apache.spark.rdd.RDD[ (Int, Double) ] = {
		var ranks: org.apache.spark.rdd.RDD[ (Int,Double) ] = matrix.map( x => (x._1, 1.0 ) );
		for( i <- 1 to iters ) {
			if( (i % (cp_freq+1)) == cp_freq ) {
				ranks.persist();
			}
			ranks = matrix.join(ranks).map( x => x._2 ).flatMap( x => x._1.map( y => (y, 0.15 + 0.85 * x._2) ) ).reduceByKey( _ + _ );
		}
		ranks
	}

	def main( args: Array[String] ): Unit = {
		if( args.length != 1 ) {
			println( "Usage: ./program_name <matrix file>." );
			return;
		}
		val conf = new SparkConf().setAppName( "Spark GraphBLAS Pagerank" )
		val sc = new SparkContext( conf );

		val t0 = System.nanoTime()
		val infile: File = new File( args(0) );
		if( !infile.exists() || infile.isDirectory()) {
			println( s"cannot access file ${args(0)}, or is a directory" )
			return
		}
		val filePath = infile.getCanonicalPath()
		println( s"reading from file ${filePath}" )


		println("Now creating GraphBLAS launcher:")
		Using( new GraphBLAS( sc ) ) { grb => {
			println("====================================")
			println("  Now running GraphBLAS PageRank")
			println("====================================")

			val t1 = System.nanoTime()
			val ( algoTime: Double, maxTime: Double ) = Using(
				PageRank.runFromFile( grb, filePath ) ) { results: PageRankResult => {
				val t2 = System.nanoTime()
				println("====================================")
				println("    GraphBLAS PageRank completed")
				println("====================================")
				val t3 = System.nanoTime()
				val maxpair = results.max()
				val t4 = System.nanoTime()
				println("maximum PageRank entry:")
				println(maxpair)
				val t21 = (t2 - t1) / 1000000000.0
				val t43 = (t4 - t3) / 1000000000.0
				( t21, t43 )
			} }.get
			val ( iters: Long, outer: Long, time: Long ) = grb.getRunStats()
			val avgTimeSecs = ( time.toDouble / outer.toDouble ) / 1000000000.0
			println( s"Iterations: $iters, outer iterations: $outer, time per outer iteration: $avgTimeSecs seconds" )
			println("GraphBLAS cleaned up.")
			val time_taken = (System.nanoTime() - t0) / 1000000000.0
			println( s"Accelerated PageRank call: $algoTime seconds." )
			println( s"Max extraction call: $maxTime seconds." )
			println( s"End-to-end time taken: $time_taken seconds." )
		} }
	}
}

