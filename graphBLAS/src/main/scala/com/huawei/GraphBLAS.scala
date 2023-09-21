
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

package com.huawei

import java.lang.Exception
import java.lang.AutoCloseable
import scala.Option
import scala.Some
import scala.reflect.ClassTag

import org.apache.spark.SparkEnv
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.TaskContext

import com.huawei.Utils
import com.huawei.CyclicPartitioner
import com.huawei.MatrixMarketReader
import com.huawei.RDDSparseMatrix

import com.huawei.graphblas.Native
import com.huawei.graphblas.PIDMapper
import com.esotericsoftware.kryo.util.Util

// @SerialVersionUID(121L)
class GraphBLAS( val sc: SparkContext ) extends AutoCloseable {

	GraphBLAS.markConstructed( this )
	var initialized: Boolean = true
	val (unique_hostnames, cluster_threads ) = GraphBLAS.getUniqueHostnames( sc )
	val distributed_rdd = sc.parallelize( 0 until cluster_threads ).map( id => (id,id) )
		.partitionBy( new CyclicPartitioner( cluster_threads ) )
	val bcmap: Broadcast[ PIDMapper ] = sc.broadcast(
			GraphBLAS.getPIDMapper(sc, cluster_threads, distributed_rdd
				.map{ pid => {Utils.getHostnameUnique() } }.collect().toArray ))
	unique_hostnames.foreach( h => {
		println( s"-> executor ${h}, threads ${bcmap.value.numThreads(h)}" )
	} )
	println( s"I detected ${bcmap.value.numProcesses} hosts." )
	println( s"I elected ${bcmap.value.headnode} as head node." )

	val instances: Array[ ( Int, Long ) ] =  {
		val bcm = bcmap
		distributed_rdd.map {
			node => {
				val hostname: String = Utils.getHostnameUnique();
				val s: Int = bcm.value.processID( hostname );
				val threads: Int = bcm.value.numThreads( hostname )
				(s, Native.begin(bcm.value.headnode, s, bcm.value.numProcesses, threads ));
			}
		}
		.filter( x => x._2 != 0 ).collect().toArray
	}
	terminateSequence()
	println("collected results:")
	instances.foreach( n => {
		println( s"host ${n._1} address: ${n._2.toHexString}" )
	})
	val bc_instances: Broadcast[ Array[( Int, Long ) ] ] = sc.broadcast( instances )

	//********************
	// Utility functions *
	//********************

	//*******************************
	//* GraphBLAS library functions *
	//*******************************

	private def terminateSequence( log: Boolean = false ): Unit = {
		val rdd = distributed_rdd.map {
			node => {
				// if( log ) {
				println( "terminating..." )
				// }
				Native.exitSequence();
				node
			}
		}
		val terminated_count = rdd.count()
		println( s"terminated ${terminated_count} nodes" )
	}

	private def logMessage( msg: String ): Unit = {
		println( msg )
	}

	private def runDistributed[ OutT : ClassTag ](
		fun: ( Int ) => OutT,
		defValue: OutT,
		filter: Option[ OutT => Boolean ],
		log: Boolean
	): Array[ OutT ] = {
		val bcm = bcmap
		val ret1: RDD[ (Int, OutT ) ] = distributed_rdd.map {
			pid => {
				val hostname: String = Utils.getHostnameUnique();
				val s: Int = bcm.value.processID( hostname );
				val canEnter = Native.enterSequence()
				if( canEnter ) {
					println( s"--------->>>>>>>>> hostname is ${hostname}" )
				}
				( if ( canEnter ) 1 else 0, if ( canEnter ) fun( s ) else defValue )
			}
		}.persist()

		if( log ) {
			val entered = ret1.map( x => x._1 ).reduce( ( a, b ) => { a + b } )
			println( s"--------->>>>>>>>> entered is ${entered}" )
		}

		val user_vals = ret1.map( x => x._2 )

		val arr: Array[ OutT ] = ( if( filter.isDefined ) { user_vals.filter( filter.get ) } else user_vals ).collect()
		ret1.unpersist()

		terminateSequence( log )
		arr
	}

	private def runDistributed[ OutT : ClassTag ](
		fun: ( Int ) => OutT,
		defValue: OutT,
		filter: OutT => Boolean,
		log: Boolean = false
	): Array[ OutT ] = runDistributed[ OutT ]( fun, defValue, Option( filter ), log )

	private def runDistributed[ OutT : ClassTag  ](
		fun: ( Int ) => OutT,
		defValue: OutT
	): Array[ OutT ] = runDistributed[ OutT ](fun, defValue, Option.empty[ OutT => Boolean ], false )

	/**
	 * Frees any underlying GraphBLAS resources.
	 *
	 * Individual #DenseVector and #SparseMatrix instances must be freed
	 * manually.
	 */
	def exit() : Unit = {
		if ( !initialized ) {
			return
		}
		val bci = bc_instances
		runDistributed( ( s: Int ) => {
			val pointer_array: Array[(Int,Long)] = bci.value.filter( _._1 == s );
			assert( pointer_array.size == 1 );
			Native.end( pointer_array(0)._2 );
		}, () )
		initialized = false
		GraphBLAS.removeConstructed( this )
		logMessage( "closing GraphBLAS" )
	}

	def close(): Unit = {
		exit()
	}

	/**
	 * Performs a pagerank computation with GraphBLAS handling matrix input.
	 *
	 * @param[in,out] sc       The Spark context.
	 * @param[in,out] instance The GraphBLAS context.
	 * @param[in]     filename The absolute path to the matrix file. This file
	 *                         must exist on each worker node, and must have the
	 *                         exact same contents.
	 *
	 * @returns A handle to the distributed PageRank vector.
	 */
	def pagerank( filename: String ) : Map[Int, Long] = {
		// val bcm = bcmap
		val bci = bc_instances
		val fun = ( s: Int ) => {
			val pointer_array: Array[(Int,Long)] = bci.value.filter( _._1 == s )
			assert( pointer_array.size == 1 )
			println(s"------->>>>>>>> s = ${s} entered")
			val ret = Native.execIO( pointer_array(0)._2, Native.PAGERANK_GRB_IO, filename )
			(s, ret)
		}
		val arr = runDistributed( fun, (-1, 0L), ( x: (Int, Long) ) => x._2 != 0L, true )
		logMessage( "Collected data instances per node:" )
		arr.foreach( l => {
			logMessage( s"node ${l._1} value ${l._2.toHexString}" )
		})
		arr.toMap
	}

	/**
	 * Retrieves the maximum value and its index from a given vector.
	 *
	 * @param[in,out] sc   The Spark context.
	 * @param[in] instance The GraphBLAS context.
	 * @param[in] vector   The vector of which to find its maximum element.
	 *
	 * @returns A pair where the first element indicates an index and the second
	 *          its value.
	 */
	def max( vector: Map[Int, Long] ) : (Long, Double) = {
		val fun = ( s: Int ) => {
			val index = Native.argmax( vector(s) );
			val value = Native.getValue( vector(s), index );
			(index, value)
		}
		val out = runDistributed( fun, (-1L, 0.0), ( x: (Long, Double ) ) => x._1 != -1L )
		val init = out(0)
		val ret: (Long, Double) = out.foldLeft( init )( (x: (Long, Double), y: (Long, Double) ) => {
				if( x._2 > y._2 ) { x } else { y }
			}
		)
		ret
	}

	def getRunStats() : (Long, Long, Long) = {
		val fun = ( s: Int ) => {
			val iters: Long = Native.getIterations();
			val outer: Long = Native.getOuterIterations();
			val time: Long = Native.getTime();
			(s, iters, outer, time)
		}
		val arr: Array[(Int, Long, Long, Long)] = runDistributed( fun, (-1, 0L, 0L, 0L),
			( x: (Int, Long, Long, Long ) ) => { x._1 == 0 && x._2 != 0 }
		)
		assert( arr.length == 1 )
		( arr( 0 )._2, arr( 0 )._3, arr( 0 )._4 )
	}

	/**
	 * Destroys a given output vector.
	 *
	 * @param[in,out] sc   The Spark context.
	 * @param[in] instance The GraphBLAS context.
	 * @param[in] vector   The vector of which to find its maximum element.
	 */
	def destroy( vector: Map[Int, Long] ) : Unit = {
		runDistributed( ( s: Int ) => {
			Native.destroyVector( vector(s) );
		}, () )
	}

	def createMatrix( file: String ): Unit /*SparseMatrix*/ = {
		// val numExecutors: Int = unique_hostnames.length;
		// val coalesced = rdd.coalesce( numExecutors );
		// val numPartitions: Int = coalesced.partitions.length;
		// if( numPartitions > numExecutors ) {
		// 	throw new java.lang.Exception( s"Coalesce did not reduce to ${numExecutors} or less parts!" );
		// }
		val matrix = MatrixMarketReader.readMM( sc, file )

		val numPartitions = matrix.data.partitions.length
		println( s"-->> num partitions for matrix: ${numPartitions}" )
		println( s"-->> number of elements: ${matrix.data.count()}, advertized ${matrix.nnz}" )

		// val bcm = bcmap
		val itemsPerPartition = matrix.data.persist().mapPartitionsWithIndex(
			( index: Int, it: Iterator[(Long,Iterable[Long],Iterable[Double])] ) => {
				// val list = it.toList
				var localNonzeroes: Long = 0L
				while( it.hasNext ) {
					val cols = it.next()._2
					localNonzeroes += cols.size
				}
				Iterator(( index, Native.addDataSeries( index, localNonzeroes ) , localNonzeroes ) )
			}
		).collect().foreach( x => {
			println( s"index ${x._1}, value ${x._2}, size ${x._3}" )
		} )

		println( "-->>> allocating memory" )

		runDistributed( ( a: Int ) => {
			Native.allocateIngestionMemory();
		}, () )

		println( "-->>> memory allocated, now adding data" )

		matrix.data.foreachPartition(
			( data: Iterator[(Long,Iterable[Long],Iterable[Double])] ) => {
				val index: Int = TaskContext.getPartitionId()
				val fieldOffset: Long = Native.getOffset()
				val baseAddress: Long = Native.getIndexBaseAddress( index )

				val unsafe = Native.getTheUnsafe()
				assert( unsafe != null )

				var offset: Long = 0L
				while( data.hasNext ){
					val rowEntry = data.next()
					val row = rowEntry._1
					var cols = rowEntry._2.iterator
					while( cols.hasNext ){
						// println( s"writing at offset ${offset}" )
						val address = baseAddress + offset
						unsafe.putLong( address, row )
						unsafe.putLong( address + fieldOffset, cols.next() )
						offset += ( 2 * fieldOffset )
					}
				}
			}
		)

		println( "-->>> now cleaning data" )

		runDistributed( ( a: Int ) => {
			Native.cleanIngestionData();
		}, () )

		// val matrix_pointers = sc.parallelize( 0 until P ).map {
		// 	pid => {
		// 		var init: Long = 0;
		// 		val hostname: String = Utils.getHostnameUnique();
		// 		if( instance._2.value.threadID( hostname ) == 0 ) {
		// 			val s: Int = instance._2.value.processID( hostname );
		// 			init = Native.matrixInput( s, P );
		// 		}
		// 		(pid,init)
		// 	}
		// }.collect().toArray;

		// val fun = ( s: Int ) => {
		// 	( s, Native.matrixInput( s, numExecutors ) )
		// }

		// val matrix_pointers = runDistributed( fun, ( -1, 0L ) ).toMap

		//note: it is *not* necessary to spawn exactly P threads here, as the call does not require communication
		//TODO FIXME in fact, there is even no need to coalesce as long as matrixAddRow is thread-safe!!
		// val init_pointers = coalesced.mapPartitionsWithIndex {
		// 	(pid, iterator) => {
		// 		var matrix_pointer: Long = 0
		// 		val hostname: String = Utils.getHostnameUnique();
		// 		if( instance._2.value.threadID( hostname ) == 0 ) {
		// 			val s = instance._2.value.processID( hostname );
		// 			matrix_pointer = matrix_pointers.filter( x => x._1 == s ).map( x => x._2 ).head;
		// 			while( iterator.hasNext ) {
		// 				val triple = iterator.next();
		// 				//Native.matrixAddRow( matrix_pointer, triple._1, triple._2.toArray, triple._2.toArray );
		// 				Native.matrixAddRow( matrix_pointer, triple._1, triple._2.toArray ); //pattern matrices only, for now
		// 			}
		// 		}
		// 		Iterator( (pid, matrix_pointer) )
		// 	}
		// };

		// val out = sc.parallelize( 0 until P ).map {
		// 	pid => {
		// 		var init_pointer: Long = 0
		// 		var matrix_pointer: Long = 0;
		// 		val hostname: String = Utils.getHostnameUnique();
		// 		if( instance._2.value.threadID( hostname ) == 0 ) {
		// 			val s = instance._2.value.processID( hostname );
		// 			init_pointer = matrix_pointers.filter( x => x._1 == s ).map( x => x._2 ).head;
		// 			matrix_pointer = Native.matrixDone( init_pointer );
		// 		}
		// 		(pid,matrix_pointer)
		// 	}
		// };

		// //done
		// new SparseMatrix( out.collect().toArray );
	}

	/**
	 * Creates a randomly generated GraphBLAS sparse matrix.
	 *
	 * @param[in] sc   The Spark context.
	 * @param[in] rows The number of desired rows.
	 * @param[in] cols The number of desired columns.
	 * @param[in] density The desired density.
	 * @param[in] variation The desired variation in row densities.
	 * @param[in] P    The number of processes to distribute over.
	 */
	/*def createRandomMatrix( sc: org.apache.spark.SparkContext, instance: Instance, rows: Long, cols: Long, density: Int, variation: Double ): SparseMatrix = {
		val rdd_in = createRandomMatrixRDD( sc, rows, cols, density, variation );
		createMatrix( sc, instance, rdd_in )
	}*/

}

object GraphBLAS {

	private val lock = new Object()

	private var context: Option[GraphBLAS] = None

	private[huawei] def markConstructed(instance: GraphBLAS): Unit = {
		lock.synchronized {
			if( context.isDefined ) {
				throw new Exception( "GraphBLAS object already defined" )
			}
			context = Some(instance)
		}
	}

	private[huawei] def removeConstructed(instance: GraphBLAS): Unit = {
		lock.synchronized {
			if( !context.isDefined ) {
				throw new Exception( "GraphBLAS object not defined" )
			}
			if( context.get ne instance ) {
				// should never get here!
				throw new Exception( "GraphBLAS object not present" )
			}
			context = None
		}
	}

	//**********************
	//* Internal functions *
	//**********************

	/**
	 * Checks if two arrays of PID - Pointer tuples compare equal.
	 *
	 * @param[in] a The first array to check in.
	 * @param[in] b The second array to check in.
	 *
	 * @returns Whether a == b.
	 */
	private def check( a: Array[(Int,Long)], b: Array[(Int,Long)] ): Boolean = {
		val ai = a.iterator;
		val bi = b.iterator;
		while( ai.hasNext ) {
			if( !bi.hasNext ) {
				return false;
			}
			if( ai.next() != bi.next() ) {
				return false;
			}
		}
		true
	}

	/**
	 * Creates a random distributed matrix.
	 *
	 * @param[in,out] sc   The Spark context.
	 * @param[in]     rows The number of rows desired.
	 * @param[in]     cols The number of columns desired.
	 * @param[in]  density The nonzero density desired.
	 *
	 * @returns An RDD of tuples. Each tuple encodes a single matrix rows and is
	 *          formatted as per the return type of #packTriples.
	 */
	/*def createRandomMatrixRDD( sc: org.apache.spark.SparkContext, rows: Long, cols: Long, density: Int, variation: Double ): RDD[(Long,Iterable[Long],Iterable[Double])] = {
		val ret = new Array[(Long, Iterable[Long], Iterable[Double])]( rows );
		var i = 0;
		println( "Constructing a random matrix. Progress (in %):" );
		while( i < rows ) {
			val rndvar: Double = (java.util.concurrent.ThreadLocalRandom.current().nextDouble() - 0.5) * variation * density.toDouble;
			var nnz: Long = density + rndvar.toLong;
			if( nnz <= 0 ) {
				nnz = 1;
			}
			if( nnz > cols ) {
				nnz = cols;
			}
			ret( i ) = ( i,
				Array.fill( nnz ) {
					java.util.concurrent.ThreadLocalRandom.current().nextLong( cols )
				},
				Array.fill( nnz ) {
					java.util.concurrent.ThreadLocalRandom.current().nextDouble()
				}
			)
			i += 1;
			if( i % (rows /10) == 0 ) {
				println( (100.0 * i.toDouble / rows.toDouble) );
			}
		}
		sc.parallelize( ret )
	}*/

	//***********************
	//* GraphBLAS Datatypes *
	//***********************

	/**
		* A GraphBLAS vector.
		*
		* \internal This is encoded as a tuple of process IDs and a pointer. The
		*           pointer is stored as a <tt>long</tt>.
		*/
	@SerialVersionUID(1L)
	class DenseVector( pointers: Array[(Int,Long)] ) extends Serializable {
		pointers.sortBy( _._1 );
		def P: Int = pointers.size
		def raw: Array[(Int,Long)] = pointers;
		def data( pid: Int ): Long = {
			pointers( pid )._2
		}
	};

	// type Instance = ( Array[String], Broadcast[PIDMapper], Array[(Int, Long)] )

	def getParallelism( sc: SparkContext ): Int = {
		println( "--> default parallelism first: " + sc.defaultParallelism )
		val seed = sc.parallelize( List(0, 1) ).map{ pid => {Utils.getHostnameUnique() } }.collect().toArray
		assert( seed.length == 2 )
		println( "--> default parallelism after: " + sc.defaultParallelism )
		sc.defaultParallelism
	}


	def getUniqueHostnames( sc: SparkContext ) : ( Array[String], Int ) = {
		val parallelism = getParallelism( sc )
		val hostnames = sc.parallelize( 0 until parallelism ).map( pid => { ( pid, pid ) } )
			.partitionBy( new CyclicPartitioner( parallelism ) )
			.mapValues( ( v: Int ) => { Utils.getHostnameUnique() } )
			.values.collect().toArray

		val unique_hostnames = hostnames.distinct
		assert( unique_hostnames.length == sc.statusTracker.getExecutorInfos.length - 1 )
		scala.util.Sorting.quickSort(unique_hostnames)
		println( "--->>> distinct executor identifiers:")
		println( unique_hostnames.toList )
		// unique_hostnames
		( unique_hostnames, parallelism )
	}

	def getPIDMapper( sc: SparkContext, P: Int, unique_hostnames: Array[ String ] ) : PIDMapper = {
		val hostnames2 = sc.parallelize( 0 until P ).map{ pid => {Utils.getHostname() } }.collect().toArray
		scala.util.Sorting.quickSort(hostnames2)
		new PIDMapper( unique_hostnames, hostnames2(0) )
	}
}
