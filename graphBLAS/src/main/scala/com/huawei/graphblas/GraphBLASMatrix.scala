package com.huawei.graphblas

import java.lang.AutoCloseable

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD

import com.huawei.graphblas.UnsafeUtils
import com.huawei.graphblas.Native
import com.huawei.RDDSparseMatrix

final class GraphBLASMatrix(
	private[graphblas] val grb: GraphBLAS,
	val rows: Long,
	val cols: Long,
	val nnz: Long,
	val grbMatAddresses: RDD[ Long ]
) extends AutoCloseable {

	var active = true

	def close(): Unit = {
		if( !active ) {
			return
		}
		println( "closing matrix" )
		grbMatAddresses.unpersist()
		grbMatAddresses.foreach( (addr: Long ) =>
			if( addr != 0L ) {
				Native.destroyMatrix( addr )
			}
		)
		active = false
	}
}

object GraphBLASMatrix {

	def createMatrixFromRDD(
		grb: GraphBLAS,
		matrix: RDDSparseMatrix,
		log: Boolean = true
	): GraphBLASMatrix = {

		val numPartitions = matrix.data.partitions.length

		if( log ) {
			println( s"-->> num partitions for matrix: ${numPartitions}" )
			println( s"-->> number of elements: ${matrix.data.count()}, advertized ${matrix.nnz}" )
		}

		val itemsPerPartition = matrix.data.persist().mapPartitionsWithIndex(
			( index: Int, it: Iterator[(Long,Iterable[Long],Iterable[Double])] ) => {
				var localNonzeroes: Long = 0L
				while( it.hasNext ) {
					localNonzeroes += it.next()._2.size
				}
				Iterator(( index, Native.addDataSeries( index, localNonzeroes ) , localNonzeroes ) )
			}
		).collect().foreach( x => {
			println( s"index ${x._1}, value ${x._2}, size ${x._3}" )
		} )

		if( log ) {
			println( "-->>> allocating ingestion memory" )
		}

		val ingestors = grb.runDistributed( ( a: Int ) => {
			Native.allocateIngestionMemory();
		}, 0L )

		if( log ) {
			println( "-->>> memory allocated, now adding data:" + ingestors.mkString(",") )
		}

		matrix.data.foreachPartition(
			( data: Iterator[(Long,Iterable[Long],Iterable[Double])] ) => {
				val index: Int = TaskContext.getPartitionId()
				val fieldOffset: Long = Native.getOffset()
				val baseAddress: Long = Native.getIndexBaseAddress( index )

				val unsafe = UnsafeUtils.getTheUnsafe()
				assert( unsafe != null )

				var offset: Long = 0L
				while( data.hasNext ){
					val rowEntry = data.next()
					val row = rowEntry._1
					var cols = rowEntry._2.iterator
					while( cols.hasNext ){
						val address = baseAddress + offset
						unsafe.putLong( address, row )
						unsafe.putLong( address + fieldOffset, cols.next() )
						offset += ( 2 * fieldOffset )
					}
				}
			}
		)

		if( log ) {
			println( "-->>> data ingested" )
		}

		val rows = matrix.m
		val cols = matrix.n
		val rdd: RDD[ Long ] = grb.runDistributedRdd( ( a: Int ) => {
				Native.ingestIntoMatrix( rows, cols )
			}, 0L
		).persist()

		// rdd.collect().foreach( el => {
		// 	println( s"   --->>> element is ${el}" )
		// }
		// )

		if( log ) {
			println( "-->>> cleaning ingestion data" )
		}

		grb.runDistributedRdd( ( a: Int ) => {
				Native.cleanIngestionData()
			}, ()
		)

		if( log ) {
			println( "-->>> ingestion data cleaned" )
		}

		new GraphBLASMatrix( grb, rows, cols, matrix.nnz, rdd )
	}

}
