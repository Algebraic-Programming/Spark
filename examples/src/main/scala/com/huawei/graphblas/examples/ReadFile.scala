
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

import com.huawei.GraphBLAS

object ReadFile {

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
		Using( new GraphBLAS( sc ) ) { grb => {

			val output = grb.createMatrix( filePath )
		} }
	}
}

