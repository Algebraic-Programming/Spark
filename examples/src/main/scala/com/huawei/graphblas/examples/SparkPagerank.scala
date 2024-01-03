
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

import scala.math.abs
import scala.math.sqrt

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel._

import com.huawei.Utils
import com.huawei.graphblas.examples.GraphMatrix
import com.huawei.graphblas.examples.cmdargs.PartitionedPageRankArgs

object SparkPagerank {

	@SerialVersionUID(100L)
	class CyclicPartitioner( P: Int, n: Long ) extends Partitioner {

		def getPartition( key: Any ): Int = {
			val k = key.asInstanceOf[Long]
			assert( k < n );
			return k.toInt % P;
		}

		def numPartitions: Int = P;
	}

	/**
	 * This is a real PageRank implementation, in that it performs proper
	 * power iterations for the Google matrix as defined in e.g. Langville
	 * and Meyer (2011). It is significantly slower than the seemingly
	 * standard variant that does not account for dangling nodes, and thus
	 * does something that is \em not mathematically equivalent to the
	 * PageRank algorithm. This alternative algorithm is implemented in
	 * another example.
	 *
	 * \warning: the PageRank vector returned is persisted to memory!
	 *
	 * @param[in] verbosity 0 is silent, 1 is warnings, 2 is info,
	 *                      3 is verbose, and 4 adds Spark RDD debug
	 *                      strings (extremely verbose).
	 */
	def flops(
		sc: SparkContext,
		matrix: GraphMatrix, alpha: Double = 0.85,
		max_iters: Int = 1000, tolerance: Double = 0.000000001,
		ckpt_freq: Int = 30, verbosity: Int = 2
	): RDD[ (Long,Double) ] = {
		val alphainv: Double = (1 - alpha);
		val alphainvcontrib: Double = ( 1 - alpha ) / matrix.n;
		val sinkIDs = sc.broadcast( matrix.sinks.collect() );

		val n = matrix.n
		val partitioner = matrix.partitioner
		var ranks: RDD[ (Long,Double) ] = sc.range( 0, matrix.n ).map( x => (x, 1.0 / n ) ).partitionBy( partitioner );

		var residual: Double = 1.0;

		var iter: Int = 1;
		while( iter <= max_iters && residual > tolerance ) {

			val oldranks = ranks;
			if( verbosity > 3 ) {
				println( oldranks.toDebugString );
			}

			var dangling: Double = 0.0;
			if( matrix.num_sinks > 0 ) {
				dangling = oldranks.filter( x => {           //x looks like (node_ID, old_pagerank_value)
					sinkIDs.value.contains( x._1 )       //select only those old values that are sinks
				} ).map( x => x._2 ).reduce( _ + _ );        //sum all old pagerank values of sink nodes
			}
			dangling = (alpha * dangling + alphainv) / matrix.n; //compute total contribution to each pagerank entry

			ranks = oldranks.join( matrix.data ).flatMap( x => { //x looks like (row_id, (input_pagerank_value, [nonzero_column_indices]))
				val numLinks: Long = x._2._2.size;           //get number of outgoing links from row_id
				x._2._2.map( y => {                          //y is a nonzero column id on row x._1
					(y, x._2._1 / numLinks)              //we output (column_id, outgoing pagerank contribution)
				} )                                          //we output an array of the above tuple
			} ).reduceByKey( _ + _ )                             //reduce all contributions to the same outlinks
			  .mapValues(
				x => {                                       //x is a double corresponding to the new pagerank so far
					alpha*x + dangling                   //we regularise the contribution
				}
			).partitionBy( matrix.partitioner).union(            //we miss contributions from dangling nodes, so take union with those
			  matrix.danglingKV.map( x => {                      //x looks like (Long,Double)
				(x._1, dangling)                                 //overwrite old value with static contribution
			  } ).partitionBy( matrix.partitioner )              //make sure the partition strategies match for the union operator
			);

			//and are done with this iteration :)
			ranks.persist( MEMORY_ONLY );

			//make sure to break very long lineages
			if( (iter-1) % (ckpt_freq+1) == ckpt_freq ) {
				ranks.checkpoint();
			}

			residual = oldranks.join(ranks).map( x => {          //x looks like (row ID, (old pagerank value, new pr value))
					abs(x._2._2 - x._2._1)               //we compute the 1-norm
				} ).sum();

			oldranks.unpersist();

			if( verbosity > 2 ) {
				println( s"  Iteration $iter: residual = $residual" );
			}

			if( verbosity > 1 && residual <= tolerance ) {
				println( s" Tolerance met: $residual (tolerance: $tolerance)." );
			}

			iter = iter + 1;
		}
		if( verbosity > 0 && residual > tolerance ) {
			println( s" Maximum iterations met! Residual at exit: $residual (tolerance: $tolerance)." );
		}
		matrix.unpersist();
		sinkIDs.destroy();
		ranks
	}

	def benchmark( sc: SparkContext, file: String, P: Int, maxIters: Int ): Unit = {

		var time = System.nanoTime()
		val matrix: GraphMatrix = GraphMatrix( sc, file, P, true )
		val read_time_taken = (System.nanoTime() - time) / 1000000000.0
		println( s"Time taken for matrix load: $read_time_taken" )

		val times: Array[Double] = new Array[Double]( 5 )

		println( "Starting dry run using default parameters..." )
		val dry_t0 = System.nanoTime()
		val maxIters: Int = 80
		val tolerance: Double = 0.0000001
		flops( sc, matrix, 0.85, maxIters, tolerance, 30, 4 ).unpersist()
		val dry_t1 = (System.nanoTime() - dry_t0) / 1000000000.0
		println( s"Time taken for dry run: $dry_t1 seconds." )

		println( "Performing benchmark of the flop-variant:" )
		for( i <- 1 to times.size ) {
			val time: Long = System.nanoTime()
			val ranks = flops( sc, matrix, 0.85, maxIters, tolerance )
			val checksum = ranks.count()
			ranks.unpersist()
			val time_taken: Double = (System.nanoTime() - time) / 1000000000.0
			times(i-1) = time_taken
			println( s" Experiment $i: $time_taken seconds. Checksum: $checksum" )
		}
		val avg_time: Double = times.sum / times.size
		val sstddev:  Double = sqrt( times.map( x => (x - avg_time) * (x - avg_time) ).sum / (times.size-1) )
		println( s"Number of runs: ${times.size}.\nAverage time taken: $avg_time seconds.\nSample standard deviation: $sstddev" )
	}

	def main( args: Array[String] ): Unit = {
		val prargs = new PartitionedPageRankArgs( args.toIndexedSeq )

		val sconf = new SparkConf().setAppName( "PageRank benchmarks" );
		val sc = new SparkContext( sconf );

		println( s"Pagerank example called with ${prargs.maxPageRankIterations()} parts for matrix and vector segments." );
		println( s"Pagerank example called using ${prargs.persistenceDirectory()} as checkpoint directory." );

		sc.setCheckpointDir( prargs.persistenceDirectory() );

		// val hostnames = sc.parallelize( 0 until prargs.numPartitions() ).map{ pid => {Utils.getHostname()} }.collect().toArray
		// val mapper = new com.huawei.graphblas.PIDMapper( sc.parallelize( 0 until prargs.numPartitions() ).map{ pid => {Utils.getHostname()} }.collect().toArray, null ) // change me!!!!!!!!!
		// val nodes = mapper.numProcesses()
		// println( s"I detected $nodes individual worker nodes." );

		prargs.forEachInputFile( x => {
			println( s"Starting benchmark using ${x}" );
			benchmark( sc, x, prargs.numPartitions(), prargs.maxPageRankIterations() );
		} );
	}

}

