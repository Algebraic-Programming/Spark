
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
import com.huawei.graphblas.examples.cmdargs.PageRankArgs


object ALPPageRankFile {

	def main( args: Array[String] ): Unit = {

		val prargs = new PageRankArgs( args.toIndexedSeq )

		val conf = new SparkConf().setAppName( "ALP-Spark GraphBLAS Pagerank from file" )
		val sc = new SparkContext( conf );

		val t0 = System.nanoTime()
		val filePath = prargs.getInputFilePath()
		println( s"reading from file ${filePath}" )


		println("Now creating GraphBLAS launcher:")
		Using.Manager( use => {
			val grb = new GraphBLAS( sc )
			println("====================================")
			println("  Now running GraphBLAS PageRank")
			println("====================================")

			val t1 = System.nanoTime()
			val results: PageRankResult = use( PageRank.runFromFile( grb, filePath, prargs.makePageRankParameters() ) )
			val t2 = System.nanoTime()
			println("====================================")
			println("    GraphBLAS PageRank completed")
			println("====================================")
			println( s"maximum PageRank entry: ${results.max()}" )
			results.perfStats
		} ).get.printStats()
	}
}

