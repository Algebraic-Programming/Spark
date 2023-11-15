
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
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.util.Using

import com.huawei.graphblas.GraphBLAS
import com.huawei.MatrixMarketReader
import com.huawei.graphblas.GraphBLASMatrix
import com.huawei.graphblas.PageRank
import com.huawei.graphblas.PageRankResult


object ALPPageRankRDD {

	def main( args: Array[String] ): Unit = {
		if( args.length != 1 ) {
			println( "Usage: ./program_name <matrix file>." );
			return;
		}
		val conf = new SparkConf().setAppName( "Spark GraphBLAS Pagerank ReadFile" )
		val sc = new SparkContext( conf );

		val infile: File = new File( args(0) );
		if( !infile.exists() || infile.isDirectory()) {
			println( s"cannot access file ${args(0)}, or is a directory" )
			return
		}
		val filePath = infile.getCanonicalPath()
		println( s"reading from file ${filePath}" )


		println("Now creating GraphBLAS launcher:")

		val ( iters: Long, outer: Long, time: Long ) =
		Using.Manager( use => {
			val grb = use( new GraphBLAS( sc ) )
			val matrix = MatrixMarketReader.readMM( sc, filePath )
			println( s"--->>>> number of partitions ${matrix.data.getNumPartitions}" )
			val grbMat = use(  GraphBLASMatrix.createMatrixFromRDD( grb, matrix, true ) )
			val results = use( PageRank.runFromMatrix( grbMat ) )
			val maxpair = results.max()
			println("maximum PageRank entry:")
			println(maxpair)
			grb.getRunStats()
		} ).get
		val avgTimeSecs = ( time.toDouble / outer.toDouble ) / 1000000000.0
		println( s"Iterations: $iters, outer iterations: $outer, time per outer iteration: $avgTimeSecs seconds" )
	}
}

