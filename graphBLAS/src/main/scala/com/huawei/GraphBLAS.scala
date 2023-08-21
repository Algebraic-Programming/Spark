
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

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast

import com.huawei.Utils
import com.huawei.graphblas.Native
import com.huawei.graphblas.PIDMapper
import scala.reflect.ClassTag

// @SerialVersionUID(121L)
class GraphBLAS( val sc: SparkContext ) extends AutoCloseable {

	GraphBLAS.markConstructed( this )
	var initialized: Boolean = true
	val (unique_hostnames, nprocs ) = GraphBLAS.getUniqueHostnames( sc )
	val bcmap: Broadcast[ PIDMapper ] = sc.broadcast( GraphBLAS.getPIDMapper(sc, nprocs, unique_hostnames ))
	println( s"I detected ${bcmap.value.numProcesses} hosts." )
	println( s"I elected ${bcmap.value.headnode} as head node." )
	val distributed_rdd = sc.parallelize( 0 until nprocs )
	val instances: Array[ ( Int, Long ) ] =  {
		val bcm = bcmap
		distributed_rdd.map {
			node => {
				val hostname: String = Utils.getHostnameUnique();
				val s: Int = bcm.value.processID( hostname );
				(s, Native.begin(bcm.value.headnode, s, bcm.value.numProcesses));
			}
		}
		.filter( x => x._2 != 0 ).collect().toArray
	}
	terminateSequence()
	println("collected results:")
	instances.foreach( n => {
		println( s"host ${n._1} address: ${n._2}" )
	})
	val bc_instances: Broadcast[ Array[( Int, Long ) ] ] = sc.broadcast( instances )

	//********************
	// Utility functions *
	//********************

	//*******************************
	//* GraphBLAS library functions *
	//*******************************

	private def terminateSequence(): Unit = {
		val terminated_count = distributed_rdd.map {
			node => {
				Native.exitSequence();
				node
			}
		}.count()
		println( s"terminated $terminated_count nodes" )
	}

	private def logMessage( msg: String ): Unit = {
		println( msg )
	}

	private def runDistributed[ OutT : ClassTag ]( fun: ( Broadcast[PIDMapper], Int, Boolean ) => OutT ): RDD[ OutT ] = {
		val bcm = bcmap
		val ret: RDD[ OutT ] = distributed_rdd.map {
			pid => {
				val hostname: String = Utils.getHostnameUnique();
				val s: Int = bcm.value.processID( hostname );
				val canEnter = Native.enterSequence()
				fun( bcm, s, canEnter )
			}
		}
		val c = ret.count()
		if( c == 0) {
			throw new Exception( "count is 0!" )
		}
		// .barrier()
		terminateSequence()
		ret
	}

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
		runDistributed( ( bcmap: Broadcast[PIDMapper], s: Int, isMain: Boolean ) => {
			if( isMain ) {
				val pointer_array: Array[(Int,Long)] = bci.value.filter( _._1 == s );
				assert( pointer_array.size == 1 );
				Native.end( pointer_array(0)._2 );
			}
		} )
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
		val fn = sc.broadcast( filename );
		val fun = ( bcmap: Broadcast[PIDMapper], s: Int, isMain: Boolean ) => {
			val ret = if( isMain ) {
				val pointer_array: Array[(Int,Long)] = bci.value.filter( _._1 == s )
				assert( pointer_array.size == 1 )
				Native.execIO( pointer_array(0)._2, Native.PAGERANK_GRB_IO, filename )
			} else 0L
			(s, ret)
		}
		val rdd_out = runDistributed( fun ).filter( x => x._2 != 0L )
		val arr = rdd_out.collect()
		logMessage( "Collected data instances per node:" )
		arr.foreach( l => {
			logMessage( s"node ${l._1} value ${l._2}" )
		})
		terminateSequence()
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
		val fun = ( bcmap: Broadcast[PIDMapper], s: Int, isMain: Boolean ) => {
			if( isMain ) {
				val index = Native.argmax( vector(s) );
				val value = Native.getValue( vector(s), index );
				(index, value)
			} else (-1L, 0.0)
		}
		val rdd_out = runDistributed( fun ).filter( x => x._1 != -1L )
		val ret: (Long, Double) = rdd_out.max()( new Ordering[Tuple2[Long,Double]]() {
			override def compare( x: (Long,Double), y: (Long,Double)): Int =
				Ordering[Double].compare(x._2, y._2)
		} )
		ret
	}

	def getRunStats() : (Long, Long, Long) = {
		val fun = ( bcmap: Broadcast[PIDMapper], s: Int, isMain: Boolean ) => {
			if( isMain ) {
				val iters: Long = Native.getIterations();
                                val outer: Long = Native.getOuterIterations();
				val time: Long = Native.getTime();
				(s, iters, outer, time)
			} else (s, 0L, 0L, 0L)
		}
		val rdd_out = runDistributed( fun ).filter( x => x._1 == 0 && x._2 != 0 )
		val arr: Array[(Int, Long, Long, Long)] = rdd_out.collect()
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
		runDistributed( ( bcmap: Broadcast[PIDMapper], s: Int, isMain: Boolean ) => {
			if( isMain ) {
				Native.destroyVector( vector(s) );
			}
		} )
	}

	/**
	 * Turns a CRS-RDD into a GraphBLAS matrix.
	 *
	 * @param[in] rdd A sparse matrix formatted as an RDD of 3-tuples. Each
	 *                3-tuple is formatted as per #packTriples.
	 * @param[in] P   The number of processes to distribute over.
	 *
	 * @returns A GraphBLAS #SparseMatrix.
	 */
	/*def createMatrix( sc: org.apache.spark.SparkContext, instance: Instance, rdd: RDD[(Long,Iterable[Long],Iterable[Double])] ): SparseMatrix = {
		val P: Int = instance._1;
		val coalesced = rdd.coalesce( P );
		val actualP: Int = coalesced.partitions.length;
		if( actualP > P ) {
			throw new java.lang.Exception( "Coalesce did not reduce to P or less parts!" );
		}

		val matrix_pointers = sc.parallelize( 0 until P ).map {
			pid => {
				var init: Long = 0;
				val hostname = "hostname"!!;
				if( instance._2.value.threadID( hostname ) == 0 ) {
					val s: Int = instance._2.value.processID( hostname );
					init = Native.matrixInput( s, P );
				}
				(pid,init)
			}
		}.collect().toArray;

		//note: it is *not* necessary to spawn exactly P threads here, as the call does not require communication
		//TODO FIXME in fact, there is even no need to coalesce as long as matrixAddRow is thread-safe!!
		val init_pointers = coalesced.mapPartitionsWithIndex {
			(pid, iterator) => {
				var matrix_pointer: Long = 0
				val hostname = "hostname"!!;
				if( instance._2.value.threadID( hostname ) == 0 ) {
					val s = instance._2.value.processID( hostname );
					matrix_pointer = matrix_pointers.filter( x => x._1 == s ).map( x => x._2 ).head;
					while( iterator.hasNext ) {
						val triple = iterator.next;
						//Native.matrixAddRow( matrix_pointer, triple._1, triple._2.toArray, triple._2.toArray );
						Native.matrixAddRow( matrix_pointer, triple._1, triple._2.toArray ); //pattern matrices only, for now
					}
				}
				Iterator( (pid, matrix_pointer) )
			}
		};

		val out = sc.parallelize( 0 until P ).map {
			pid => {
				var init_pointer: Long = 0
				var matrix_pointer: Long = 0;
				val hostname = "hostname"!!;
				if( instance._2.value.threadID( hostname ) == 0 ) {
					val s = instance._2.value.processID( hostname );
					init_pointer = matrix_pointers.filter( x => x._1 == s ).map( x => x._2 ).head;
					matrix_pointer = Native.matrixDone( init_pointer );
				}
				(pid,matrix_pointer)
			}
		};

		//done
		new SparseMatrix( out.collect().toArray );
	}*/

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

	/**
		* A GraphBLAS matrix.
		*
		* \internal This is encoded as a tuple of process IDs and a pointer. The
		*           pointer is stored as a <tt>long</tt>.
		*/
	@SerialVersionUID(1L)
	class SparseMatrix( pointers: Array[(Int,Long)] ) extends Serializable {
		pointers.sortBy( _._1 );
		def P: Int = pointers.size
		def raw: Array[(Int,Long)] = pointers;
		def data( pid: Int ): Long = {
			pointers( pid )._2
		}
	};

	type Instance = ( Array[String], Broadcast[PIDMapper], Array[(Int, Long)] )

	def getParallelism( sc: SparkContext ): Int = {
		println( "--> default parallelism first: " + sc.defaultParallelism )
		val seed = sc.parallelize( List(0, 1) ).map{ pid => {Utils.getHostnameUnique() } }.collect().toArray
		println( "--> default parallelism after: " + sc.defaultParallelism )
		sc.defaultParallelism
	}


	def getUniqueHostnames( sc: SparkContext ) : ( Array[String], Int ) = {
		val parallelism = getParallelism( sc )
		val hostnames_prior = sc.parallelize( 0 until parallelism ).map{ pid => {Utils.getHostnameUnique() } }.collect().toArray
		// hostnames_prior.foreach( { s =>
		// 	println( s"hostname is: $s" )
		// } )
		// println("keys")
		// val hosts = sc. statusTracker.getExecutorInfos.map( i => i.host )
		// println(hosts.toList)

		// val hostnames = sc.parallelize( hosts.toSeq ).map{ pid => {Utils.getHostnameUnique() } }.collect().toArray
		// println( "--> default parallelism 3: " + sc. defaultParallelism )

		// println( "--->>> hostnames:")
		// println( hostnames.toList )
		val unique_hostnames = hostnames_prior.distinct
		scala.util.Sorting.quickSort(unique_hostnames)
		println( "--->>> distinct hostnames:")
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
