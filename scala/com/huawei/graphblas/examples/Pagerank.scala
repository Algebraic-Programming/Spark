
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

import sys.process._

import org.apache.spark.SparkEnv
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import com.huawei.GraphBLAS

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
		if( args.length != 2 ) {
			println( "Usage: ./program_name <P> <matrix file>." );
			return;
		}
		val sc = new SparkContext( new SparkConf().setAppName( "Spark GraphBLAS Pagerank" ) );
		val P = args(0).toInt;
		println( s"Using P = $P." );
		val t0 = System.nanoTime();
		val hostnames = sc.parallelize( 0 until P ).map{ pid => {(SparkEnv.get.executorId,"hostname"!!)} }.collect().toArray
		println("Manual hostnames gathering at start of example:")
		println(hostnames.deep)
		println("Now creating GraphBLAS launcher:")
		val grb = com.huawei.GraphBLAS.initialize( sc, P )
		println("grb instance contents:")
		println(grb)
		val t1 = System.nanoTime();
		val output = com.huawei.GraphBLAS.pagerank( sc, grb, args(1) );
		val t2 = System.nanoTime();
		println("output contents:")
		println(output)
		val t3 = System.nanoTime();
		val maxpair = com.huawei.GraphBLAS.max( sc, grb, output );
		val t4 = System.nanoTime();
		println("maximum PageRank entry:");
		println(maxpair);
		com.huawei.GraphBLAS.destroy( sc, grb, output );
		com.huawei.GraphBLAS.exit( sc, grb )
		println("GraphBLAS cleaned up.")
		val time_taken = (System.nanoTime() - t0) / 1000000000.0;
		val t21 = (t2 - t1) / 1000000000.0;
		val t43 = (t4 - t3) / 1000000000.0;
		println( s"Accelerated PageRank call: $t21 seconds." );
		println( s"Max extraction call: $t43 seconds." );
		println( s"End-to-end time taken: $time_taken seconds." );
	}
}

