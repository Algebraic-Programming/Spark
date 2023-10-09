
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

import org.apache.spark.SparkEnv
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.util.Using

import com.huawei.Utils
import com.huawei.graphblas.GraphBLAS

object Initialise {

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
		val conf = new SparkConf().setAppName( "Spark GraphBLAS Initialise" )

		val sc = new SparkContext( conf );

		val t0 = System.nanoTime();

		println("Now creating GraphBLAS launcher:")
		Using( new GraphBLAS( sc ) ) { grb => {
			println("grb instance contents:")
			println(grb)
			// println("GraphBLAS cleaned up.")
			val time_taken = (System.nanoTime() - t0) / 1000000000.0;
			println( s"Accelerator time taken: $time_taken." );
		}}
	}
}

