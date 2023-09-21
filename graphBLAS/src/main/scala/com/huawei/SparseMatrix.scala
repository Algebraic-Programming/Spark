
package com.huawei

/**
* A GraphBLAS matrix.
*
* \internal This is encoded as a tuple of process IDs and a pointer. The
*           pointer is stored as a <tt>long</tt>.
*/
class SparseMatrix( pointers: Array[(Int,Long)] ) {
	pointers.sortBy( _._1 );
	def P: Int = pointers.size
	def raw: Array[(Int,Long)] = pointers;
	def data( pid: Int ): Long = {
		pointers( pid )._2
	}
}
