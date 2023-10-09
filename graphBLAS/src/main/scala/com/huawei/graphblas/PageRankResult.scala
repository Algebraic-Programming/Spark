package com.huawei.graphblas

import java.lang.AutoCloseable

import org.apache.spark.rdd.RDD

import com.huawei.graphblas.GraphBLAS
import com.huawei.graphblas.Native


final class PageRankResult( private val grb: GraphBLAS, val vector: RDD[ Long ] ) extends AutoCloseable {

	var active = true

		/**
	 * Destroys a given output vector.
	 *
	 * @param[in,out] sc   The Spark context.
	 * @param[in] instance The GraphBLAS context.
	 * @param[in] vector   The vector of which to find its maximum element.
	 */
	def close(): Unit = {
		println( "closing PageRank result" )
		if( !active ) {
			return
		}
		vector.foreach( ( address: Long ) => {
			if( address != 0L ) {
				Native.destroyVector( address );
			}
		} )
		active = false
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
	def max() : (Long, Double) = {
		val init = ( Long.MinValue, Double.MinValue )
		vector.map( ( address: Long ) => {
			if( address != 0L ) {
				val index = Native.argmax( address );
				val value = Native.getValue( address, index );
				(index, value)
			} else init
		}
		).fold( init )( (x: (Long, Double), y: (Long, Double) ) => {
				if( x._2 > y._2 ) { x } else { y }
			}
		)
	}

}
