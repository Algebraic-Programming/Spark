
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

package com.huawei.graphblas

import java.lang.Exception
import java.lang.AutoCloseable
import scala.Option
import scala.Some
import scala.reflect.ClassTag

import org.apache.spark.SparkEnv
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast

import com.huawei.Utils
import com.huawei.CyclicPartitioner
import com.huawei.MatrixMarketReader
import com.huawei.RDDSparseMatrix

import com.huawei.graphblas.Native
import com.huawei.graphblas.PIDMapper


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

	private[graphblas] def runDistributedRdd[ OutT : ClassTag ](
		fun: ( Int ) => OutT,
		defValue: OutT,
		filter: Option[ OutT => Boolean ],
		log: Boolean
	): RDD[ OutT ] = {
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
		terminateSequence( log )

		if( log ) {
			val entered = ret1.map( x => x._1 ).reduce( ( a, b ) => { a + b } )
			println( s"--------->>>>>>>>> entered is ${entered}" )
		}

		val user_vals = ret1.map( x => x._2 )
		( if( filter.isDefined ) { user_vals.filter( filter.get ) } else user_vals ).unpersist()
	}

	private[graphblas] def runDistributedRdd[ OutT : ClassTag ](
		fun: ( Int ) => OutT,
		defValue: OutT,
		filter: OutT => Boolean,
		log: Boolean
	): RDD[ OutT ] = runDistributedRdd[ OutT ]( fun, defValue, Option( filter ), log )

	private[graphblas] def runDistributedRdd[ OutT : ClassTag ](
		fun: ( Int ) => OutT,
		defValue: OutT,
		log: Boolean = false
	): RDD[ OutT ] = runDistributedRdd( fun, defValue, Option.empty[ OutT => Boolean ], log )

	private[graphblas] def runDistributed[ OutT : ClassTag ](
		fun: ( Int ) => OutT,
		defValue: OutT,
		filter: Option[ OutT => Boolean ],
		log: Boolean
	): Array[ OutT ] = {
		val user_vals = runDistributedRdd( fun, defValue, filter, log )
		user_vals.collect()
	}

	private[graphblas] def runDistributed[ OutT : ClassTag ](
		fun: ( Int ) => OutT,
		defValue: OutT,
		filter: OutT => Boolean,
		log: Boolean = false
	): Array[ OutT ] = runDistributed[ OutT ]( fun, defValue, Option( filter ), log )

	private[graphblas] def runDistributed[ OutT : ClassTag  ](
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

	def getRunStats() : (Long, Long, Long) = {
		val fun = ( s: Int ) => {
			val iters: Long = Native.getIterations();
			val outer: Long = GraphBLAS.outerIterations;
			val time: Long = Native.getTime();
			(s, iters, outer, time)
		}
		val arr: Array[(Int, Long, Long, Long)] = runDistributed( fun, (-1, 0L, 0L, 0L),
			( x: (Int, Long, Long, Long ) ) => { x._1 == 0 && x._2 != 0 }
		)
		assert( arr.length == 1 )
		( arr( 0 )._2, arr( 0 )._3, arr( 0 )._4 )
	}
}


object GraphBLAS {

	final val outerIterations = 5

	private val lock = new Object()

	private var context: Option[GraphBLAS] = None

	private[graphblas] def markConstructed(instance: GraphBLAS): Unit = {
		lock.synchronized {
			if( context.isDefined ) {
				throw new Exception( "GraphBLAS object already defined" )
			}
			context = Some(instance)
		}
	}

	private[graphblas] def removeConstructed(instance: GraphBLAS): Unit = {
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
