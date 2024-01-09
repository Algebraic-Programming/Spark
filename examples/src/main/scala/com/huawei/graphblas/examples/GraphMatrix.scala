
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

import scala.math.max

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.HashPartitioner
import org.apache.spark.storage.StorageLevel._

/**
	* Parses square non-symmetric MatrixMarket or csv files as
	* pattern matrices.
	*
	* Ignores the header line. Ignores nonzero values.
	* Ignores all lines starting with '%'.
	*/
// @SerialVersionUID(100L)
class GraphMatrix(
	val m: Long, val n: Long, val nnz: Long,
	val data: RDD[ (Long,Iterable[Long]) ], val sinks: RDD[ Long ], val dangling: RDD[ Long ],
	val num_sinks: Long,
	val danglingKV: RDD[ (Long,Double) ],
	val partitioner: HashPartitioner
) {
	

	def persist(): Unit = {
		data.persist( MEMORY_AND_DISK )
		sinks.persist( MEMORY_ONLY )
		dangling.persist( MEMORY_ONLY )
	}

	def unpersist(): Unit = {
		data.unpersist()
		sinks.unpersist()
		dangling.unpersist()
	}
}

object GraphMatrix {
	def apply(sc: SparkContext, filepath: String, P: Int, check: Boolean = false, _sep: String = "default"): GraphMatrix = {
		val sep = if( _sep == "default" ) {
			if( filepath.endsWith( ".mtx" ) ) " "
			else if( filepath.endsWith( ".csv" ) ) ","
			else {
				println( "No explicit separator character given and could not derive a default from the filename." );
				" "
			}
		} else _sep
		println( s"Using `$sep' as separator." )
		val filtered: RDD[ String ] = sc.textFile( filepath ).filter( x => ! x.startsWith("%") )
		val nlines = if( check == true ) {
			val time = System.nanoTime()
			filtered.persist( MEMORY_AND_DISK )
			val _nlines = filtered.count()
			val time_taken = (System.nanoTime() - time) / 1000000000.0
			println( s"Pure read IO time taken: $time_taken seconds." )
			_nlines
		} else 0L
		var data: RDD[ (Long, Iterable[Long]) ] =
			filtered.map( x => {                               //x is a bunch of strings where each line is formatted as "i,j,v" or "i,j"
				val line = x.split( sep )                 //create a 2 or 3 tuple
				assert( line.size == 2 || line.size == 3 )
				(line(0).toLong - 1, line(1).toLong - 1)   //convert to coordinate 2-tuple
			} ).groupByKey();                                 //create a row-major storage RDD where each element is (row_id, [column_ids])

		// var data = createData( filtered, sep )

		//we are going to derive some statistics from the matrix file, so persist it
		data.persist( MEMORY_AND_DISK )
		if( check == true ) {
			//we no longer required filtered as we have the data RDD
			filtered.unpersist();
		}
		val nnz: Long = data.map( x => x._2.size ).reduce( _ + _ )
		val m:   Long = data.map( x => x._1 ).reduce( max ) + 1
		var dangling = data.flatMap( x => x._2 ).distinct()  //get a set of unique column indices in the matrix
		val n:   Long = max( m, dangling.reduce( max ) + 1 ) //use dangling node discovery to deduce last few statistics.
		var sinks = data.map( x => x._1 ).distinct()         //get a set of unique row indices in the matrix
		data.unpersist()                                     //we are done with gathering statistics on the matrix itself,
		assert( m == n )                                     //pagerank needs square matrices
		sinks = sc.range( 0, n ).subtract( sinks )           //this is an RDD with element type Long, storing the sink IDs.
		dangling = sc.range( 0, n ).subtract( dangling )     //this is an RDD with element type Long, storing the dangling node IDs.

		//make sure all (Key,Value) RDDs are partitioned equally
		//val partitioner: CyclicPartitioner = new CyclicPartitioner( P, n );
		val partitioner = new HashPartitioner( P )
		println( data.toDebugString )
		data = data.partitionBy( partitioner )
		data = data.repartition( P )
		val danglingKV = dangling.map( x => (x, 0.0) )        //cache RDD of (Long,Double) to account for contributions to dangling nodes.
			.partitionBy( partitioner )                        //make sure its partitioning equals that of data
		// persist()
		data.checkpoint()
		danglingKV.checkpoint()
		sinks.checkpoint()

		val num_dangling = dangling.count()                  //count number of dangling nodes
		val num_sinks    = sinks.count()                     //count number of sink nodes

		println( s"I have parsed $filepath as an $m times $n matrix containing $nnz nonzeroes. It contains $num_dangling dangling nodes and $num_sinks sink nodes." )
		if( check && nnz != nlines ) {
			println( s"Warning: number of non-comment lines in $filepath ($nlines) does not match number of nonzeroes read ($nnz)" )
		}
		val res = new GraphMatrix( m, n, nnz, data, sinks, dangling, num_sinks, danglingKV, partitioner )
		res.persist()
		res
	}
}
