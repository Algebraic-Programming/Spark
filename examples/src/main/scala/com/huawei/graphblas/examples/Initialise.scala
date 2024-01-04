
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

	def main( args: Array[String] ): Unit = {
		val conf = new SparkConf().setAppName( "Spark GraphBLAS Initialise" )

		val sc = new SparkContext( conf );

		val t0 = System.nanoTime();

		println("Now creating GraphBLAS launcher:")
		Using( new GraphBLAS( sc ) ) { grb => {
			println("GraphBLAS instance contents:")
			println(grb)
			val time_taken = (System.nanoTime() - t0) / 1000000000.0;
			println( s"Accelerator time taken: $time_taken." );
		}}
		println("GraphBLAS cleaned up.")
	}
}
