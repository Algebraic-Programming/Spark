
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

	def spark_pr( matrix: RDD[ (Long, Iterable[Long] ) ], iters: Int, cp_freq: Int = 30 ): RDD[ (Long, Double) ] = {
		var ranks: RDD[ (Long, Double) ] = matrix.map( x => (x._1, 1.0 ) );
		for( i <- 1 to iters ) {
			if( (i % (cp_freq+1)) == cp_freq ) {
				ranks.persist();
			}
			ranks = matrix.join(ranks).map( x => x._2 ).flatMap( x => x._1.map( y => (y, 0.15 + 0.85 * x._2) ) ).reduceByKey( _ + _ );
		}
		ranks
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
		val iters = args(2).toInt;
		val infile: File = new File( args(3) );
		if( !infile.exists() || infile.isDirectory()) {
			println( s"cannot access file ${args(3)}, or is a directory" )
			return
		}
		val filePath = infile.getCanonicalPath()
		println( s"reading from file ${filePath}" )

		sc.setCheckpointDir( chkptdir );
		val matrix: GraphMatrix = new GraphMatrix( sc, filePath, P, true )

		println( s"Pagerank example called with $P parts for matrix and vector segments." );
		println( s"Pagerank example called using $chkptdir as checkpoint directory." );

		val result = spark_pr( matrix.data, iters )
	}

}