
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
import scala.Option
import scala.Some

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast

import com.huawei.Utils
import com.huawei.graphblas.Native
import com.huawei.graphblas.PIDMapper

// @SerialVersionUID(121L)
class GraphBLAS( val sc: SparkContext, val P: Int ) {

	GraphBLAS.markConstructed( this )
	val unique_hostnames : Array[ String ] = GraphBLAS.getUniqueHostnames( sc, P )
	val bcmap: Broadcast[ PIDMapper ] = sc.broadcast( GraphBLAS.getPIDMapper(sc, P, unique_hostnames ))
	println( s"I detected ${bcmap.value.numProcesses} hosts." )
	println( s"I elected ${bcmap.value.headnode} as head node." )
	val distributed_rdd = sc.parallelize( 0 until P )
	val instances: Array[ ( Int, Long ) ] =  {
		val bcm = bcmap
		distributed_rdd.map {
			node => {
				val hostname: String = Utils.getHostnameUnique;
				val s: Int = bcm.value.processID( hostname );
				(s, Native.begin(bcm.value.headnode, s, bcm.value.numProcesses));
			}
		}
		.filter( x => x._2 != 0 ).collect.toArray
	}
	terminateSequence
	println("collected results:")
	instances.foreach( n => {
		println( s"host ${n._1} address: ${n._2}" )
	})
	val bc_instances: Broadcast[ Array[( Int, Long ) ] ] = sc.broadcast( instances )

	//********************
	// Utility functions *
	//********************

	/**
		* Reads a non-symmetric MatrixMarket file and interprets it as a pattern
		* matrix, thus ignoring any nonzero values that may be in the file.
		*
		* @param[in] sc The Spark context.
		* @param[in] fn The path to the MatrixMarket file.
		*
		* @returns Nonzeroes are grouped by matrix row-- the first entry is a row
		*          index, while the second entry is an array of column indices with
		*          nonzeroes on that row.
		*/
	def readPatternMatrix( fn: String, P: Int = 0 ) : org.apache.spark.rdd.RDD[ (Long, Iterable[Long] ) ] = {
		val file = if(P == 0) sc.textFile(fn) else sc.textFile(fn,P);
		file.filter( word => word(0) != '%' ).mapPartitionsWithIndex{ (idx,iter) => if (idx == 0 ) iter.drop(1) else iter}.map( x => x.split( " " ) ).map( x => (x(0).toLong,x(1).toLong) ).groupBy( x => x._1 ).map( x => (x._1, x._2.map( y => y._2 )) )
	}

	/**
		* Reads a non-symmetric MatrixMarket file.
		*
		* @param[in] sc The Spark context.
		* @param[in] fn The path to the MatrixMarket file.
		*
		* @returns A 4-tuple. The first entry contains the number of matrix rows,
		*          the second the number of matrix columns, the third the number of
		*          nonzeroes. The fourth entry is an RDD of 3-tuples formatted as per
		*          #packTriples.
		*/
	/*def readCoordMatrix( sc: org.apache.spark.SparkContext, fn: String ) : (Long, Long, Long, org.apache.spark.rdd.RDD[ (Long, Iterable[Long], Iterable[Double]) ]) = {
		val fnRDD = sc.textFile( fn );
		val header = parseHeader( fnRDD );
		val parsed = filterHeader( fnRDD );
		val ret = parsed.filter( x => !x.startsWith("%") ).map( parseTriple ).groupBy( x => x._1 ).map( x => packTriples(x._1, x._2) );
		(header._1, header._2, header._3, ret)
	}*/

	/**
		* Reads a symmetric MatrixMarket file.
		*
		* @see #readCoordMatrix.
		*/
	/*def readSymCoordMatrix( sc: org.apache.spark.SparkContext, fn: String ) : (Long, Long, Long, org.apache.spark.rdd.RDD[ (Long, Iterable[Long], Iterable[Double]) ]) = {
		val fnRDD = sc.textFile( fn );
		val header = parseHeader( fnRDD );
		val parsed = filterHeader( fnRDD );
		val ret = parsed.filter( x => !x.startsWith("%") ).flatMap( parseSymTriple ).groupBy( x => x._1 ).map( x => packTriples(x._1, x._2) )
		(header._1, header._2, header._3, ret)
	}*/


	//*******************************
	//* GraphBLAS library functions *
	//*******************************

	def terminateSequence(): Unit = {
		val terminated_count = distributed_rdd.map {
			node => {
				val hostname: String = Utils.getHostnameUnique();
				Native.exitSequence();
				hostname
			}
		}.count
		println( s"terminated $terminated_count nodes" )
	}



	/**
		* Frees any underlying GraphBLAS resources.
		*
		* Individual #DenseVector and #SparseMatrix instances must be freed
		* manually.
		*/
	def exit() : Unit = {
		val bcm = bcmap
		val bci = bc_instances
		val rdd_out = distributed_rdd.map {
			node => {
				val hostname: String = Utils.getHostnameUnique();
				val s: Int = bcm.value.processID( hostname );
				if( Native.enterSequence() ) {
					val pointer_array: Array[(Int,Long)] = bci.value.filter( _._1 == s );
					assert( pointer_array.size == 1 );
					Native.end( pointer_array(0)._2 );
				}
			}
		}
		terminateSequence
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
	def pagerank( filename: String ) : GraphBLAS.DenseVector = {
		val bcm = bcmap
		val bci = bc_instances
		val fn = sc.broadcast( filename );
		val rdd_out = distributed_rdd.map(
			pid => {
				val hostname: String = Utils.getHostnameUnique();
				val s: Int = bcm.value.processID( hostname );
				var ret: Long = 0;
				if( Native.enterSequence() ) {
					val pointer_array: Array[(Int,Long)] = bci.value.filter( _._1 == s );
					assert( pointer_array.size == 1 );
					ret = Native.execIO( pointer_array(0)._2, Native.PAGERANK_GRB_IO, fn.value );
				}
				(s, ret)
			}
		)
		val v = new GraphBLAS.DenseVector( rdd_out.collect() )
		terminateSequence
		v
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
	def max( vector: GraphBLAS.DenseVector ) : (Long, Double) = {
		val bcm = bcmap
		val rdd_out = distributed_rdd.map {
			pid => {
				val hostname: String = Utils.getHostnameUnique();
				val s: Int = bcm.value.processID( hostname );
				var index: Long = 0;
				var value: Double = 0.0;
				if( Native.enterSequence() ) {
					index = Native.argmax( vector.data(s) );
					value = Native.getValue( vector.data(s), index );
				}
				(index,value)
			}
		}
		terminateSequence
		rdd_out.max()( new Ordering[Tuple2[Long,Double]]() {
			override def compare( x: (Long,Double), y: (Long,Double)): Int =
				Ordering[Double].compare(x._2, y._2)
		} )
	}

	/**
		* Destroys a given output vector.
		*
		* @param[in,out] sc   The Spark context.
		* @param[in] instance The GraphBLAS context.
		* @param[in] vector   The vector of which to find its maximum element.
		*/
	def destroy( vector: GraphBLAS.DenseVector ) : Unit = {
		val bcm = bcmap
		val rdd_out = distributed_rdd.map {
			pid => {
				val hostname: String = Utils.getHostnameUnique();
				val s: Int = bcm.value.processID( hostname );
				if( Native.enterSequence() ) {
					Native.destroyVector( vector.data(s) );
				}
			}
		}
		terminateSequence
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
		if( context.isDefined) {
			throw new Exception( "GraphBLAS object already defined" )
		}
      context = Some(instance)
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
			if( ai.next != bi.next ) {
				return false;
			}
		}
		true
	}

	/**
		* Parses the header of a MatrixMarket file.
		*
		* @param[in] in The MatrixMarket file as an RDD of strings.
		*
		* @returns A 3-tuple \f$ (m,n,nnz) \f$.
		*/
	private def parseHeader( in: org.apache.spark.rdd.RDD[String] ): (Long, Long, Long) = {
		val subRDD = in.mapPartitions( iterator => {
			val first = iterator.take(1);
			if( first.hasNext ) {
				if( first.next.startsWith( "%" ) ) {
					val filtered = iterator.dropWhile( x => x.startsWith( "%" ) );
					assert( filtered.hasNext ); //this could in principle fail if you are very very very unlucky. In that case, change the number of parts (decrease by 1, e.g.) and you should be fine.
					val sz_header = filtered.next.split( " " );
					println( sz_header.flatten )
					assert( sz_header.size == 3 );
					Iterator((sz_header.apply(0).toLong, sz_header.apply(1).toLong, sz_header.apply(2).toLong))
				} else {
					Iterator()
				}
			} else {
				Iterator()
			}
		} );
		subRDD.max()
	}

	/**
		* Strips the header from a MatrixMarket file.
		*
		* @param[in] in The MatrixMarket file as an RDD of strings.
		*
		* @returns An RDD of strings with the contents of the MatrixMarket file,
		*          without its header line.
		*/
	private def filterHeader( in: RDD[String] ): RDD[String] = {
		in.mapPartitions( iterator => {
			if( iterator.hasNext ) {
				var line = iterator.next;
				if( line.startsWith( "%" ) ) {
					val filtered = iterator.dropWhile( x => x.startsWith( "%" ) );
					assert( filtered.hasNext ); //see above comment
					filtered.next;
					filtered
				} else {
					Iterator( line ) ++ iterator
				}
			} else {
				Iterator()
			}
		} )
	}

	/**
		* Given a 3-tuple in string form, translate it into a coordinate-value pair.
		*
		* @param[in] x A string consisting of a single line, two positive non-zero
		*              integers separated and followed by a space, and one floating
		*              point value closing off the string.
		*
		* @returns A 3-tuple (i,j,v), where (i,j) are the two integers (in the same
		*          order) and v is the floating point value in double precision.
		*/
	private def parseTriple( x: String ) : (Long, Long, Double) = {
		x.split(" ") match {
			case Array( a, b, c ) => (a.toLong - 1, b.toLong - 1, c.toDouble)
			case Array( a, b ) => (a.toLong - 1, b.toLong - 1, 1.0)
			case _ => throw new java.lang.Exception( "Cannot parse matrix market file at line " + x );
		}
	}

	/**
		* Given a 3-tuple in string form, translate it into one or two coordinate-
		* value pairs. One pair is returned if and only if its coordinates are
		* equal; otherwise both the original coordinate-value pair is joined by its
		* transposed nonzero.
		*
		* @param[in] x A string consisting of a single line, two positive non-zero
		*              integers separated and followed by a space, and one floating
		*              point value closing off the string.
		*
		* @returns If the two integers are equal: a 3-tuple (i,i,v), where (i,i) are
		*          the two integers and v is the floating point value in double
		*          precision. The tuple is packed in an array of size one.
		* @returns Otherwise: an array with two 3-tuples (i,j,v) and (j,i,v).
		*/
	private def parseSymTriple( x: String ) : Array[(Long, Long, Double)] = {
		val orig = parseTriple(x);
		if( orig._1 == orig._2 ) {
			Array( orig )
		} else {
			Array( orig, (orig._2, orig._1, orig._3) )
		}
	}

	/**
		* Turns an array of 3-tuples where the first element of each tuple is equal
		* into an array of 2-tuples.
		*
		* @param[in] rowID The row index. Each first element of each given tuple is
		*                  equal to this value.
		* @param[in] x     An array of 3-tuples of which each first entry should
		*                  equal \a rowID.
		*
		* @returns A 3-tuple where the first entry equals \a rowID while the second
		*          entry is an array of column indices (the second entry of each
		*          input tuple) and the third entry is an array of values (the third
		*          entry of each input tuple). The order of array entries is
		*          retained.
		*
		* For example: [(1,2,3.5),(1,5,0.1),(1,3,1.1)] is turned into
		*              (1,[2 5 3], [3.5 0.1 1.1])
		*/
	private def packTriples( rowID: Long, x: Iterable[(Long, Long, Double)] ) : (Long, Iterable[Long], Iterable[Double]) = {
		val unzipped : (Iterable[Long], Iterable[Double]) = x.map( x => (x._2, x._3) ).unzip;
		(rowID, unzipped._1, unzipped._2)
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


	def getUniqueHostnames( sc: SparkContext, P: Int ) : Array[String] = {
		val hostnames_prior = sc.parallelize( 0 until P ).map{ pid => {Utils.getHostnameUnique() } }.collect().toArray
		hostnames_prior.foreach( { s =>
			println( s"hostname is: $s" )
		} )
		println("keys")
		val hosts = sc. statusTracker.getExecutorInfos.map( i => i.host )
		println(hosts.toList)

		val hostnames = sc.parallelize( hosts ).map{ pid => {Utils.getHostnameUnique() } }.collect().toArray

		println( "--->>> hostnames:")
		println( hostnames.toList )
		val unique_hostnames = hostnames.distinct
		scala.util.Sorting.quickSort(unique_hostnames)
		println( "--->>> distinct hostnames:")
		println( unique_hostnames.toList )
		unique_hostnames
	}

	def getPIDMapper( sc: SparkContext, P: Int, unique_hostnames: Array[ String ] ) : PIDMapper = {
		val hostnames2 = sc.parallelize( 0 until P ).map{ pid => {Utils.getHostname() } }.collect().toArray
		scala.util.Sorting.quickSort(hostnames2)
		new PIDMapper( unique_hostnames, hostnames2(0) )
	}
}
