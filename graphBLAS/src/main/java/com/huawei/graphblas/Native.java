
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


package com.huawei.graphblas;


import java.io.Serializable;
import sun.misc.Unsafe;
import java.lang.reflect.Field;


/** Collects the native interface into the GraphBLAS. */
public class Native implements Serializable {

	static {
		System.loadLibrary( "sparkgrb" );
	}

	/** Version data used for serialisation. */
	private static final long serialVersionUID = 1L;

	/** Supported GraphBLAS program: PageRank. */
	public static final int PAGERANK_GRB_IO = 0;

	/**
	 * Initialises the GraphBLAS back-end.
	 *
	 * None of the functions in #Native may be called until this function is called
	 * first.
	 *
	 * @param[in] s This process ID.
	 * @param[in] P The total number of user processes collectively calling this
	 *              function. These processes shall form the current parallel
	 *              context; all functions in this #Native class shall be called
	 *              collectively by all user processes in the context.
	 *
	 * @returns An instance pointer used to start GraphBLAS programs.
	 */
	public static long begin( String master, int s, int P, int threads ) throws Exception {
		System.out.println("number of processes: " + Runtime.getRuntime().availableProcessors() );
		boolean isMain = Native.enterSequence();
		if( isMain ) {
			System.out.println("initialization for node " + s );
			return Native.start( master, s, P, threads );
		} else return 0L;
	}

	/** @see #begin -- this implements the native part of its functionality. */
	private static native long start( String master, int s, int P, int threads );

	/**
	 * Finalises the current GraphBLAS context.
	 *
	 * After a call to this function, none of the functions in #Native may be
	 * called again until a call to #init has been made. Only #init may be called
	 * after calling this function.
	 *
	 * @param[in] instance The instance pointer as returned by #begin.
	 *
	 * After a call to this function \a instance shall no longer be valid. A new
	 * call to #begin may, however, and of course, be made.
	 */
	public static native void end( long instance );

	/**
	 * Instructs the GraphBLAS to create a matrix from an input MatrixMarket file.
	 *
	 *
	 * \warning The file path must be available on each node this code executes on.
	 *          Use input via RDDs if you cannot guarantee this.
	 *
	 * @param[in] s The local process ID.
	 * @param[in] P the total number of user processes calling this function.
	 * @param[in] path Path to the MatrixMarket file.
	 *
	 * @returns A pointer to the new GraphBLAS matrix.
	 */
	// public static native long createMatrix( int s, int P, String path );




	public static native long addDataSeries( int index, long length );

	public static native void allocateIngestionMemory();

	public static native long getOffset();

	public static native long getIndexBaseAddress( int index );


	public static Unsafe getTheUnsafe() {
		sun.misc.Unsafe unsafe;
		try {
			Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
			unsafeField.setAccessible(true);
			unsafe = (sun.misc.Unsafe) unsafeField.get(null);
		} catch (Throwable cause) {
			unsafe = null;
		}
		return unsafe;
	}

	public static native void cleanIngestionData();


	/**
	 * Initialises streaming in a sparse matrix.
	 *
	 * @param[in] s The local matrix ID.
	 * @param[in] P The total number of user processes calling this function.
	 *
	 * @returns A local pointer to a matrix-under-construction.
	 */
	// public static native long matrixInput( int s, int P );

	/**
	 * Adds one row to a matrix under construction.
	 *
	 * @param[in,out] matrixInput A pointer as given by a call to #matrixInput.
	 * @param[in] row The row that is currently being added.
	 * @param[in] col_ind The array of column indices this row has nonzeroes on.
	 * @param[in] values The array of nonzeroes this row contains.
	 */
	// public static native void matrixAddRow( long matrix, long row, long col_ind[] );

	/**
	 * Finalises reading in a sparse matrix.
	 *
	 * @param[in,out] matrixInput A pointer as given by a call to #matrixInput.
	 *                            The same pointer may have been given multiple
	 *                            times to the #matrixAddRow function.
	 *
	 * @returns A pointer to the finalised GraphBLAS matrix.
	 */
	// public static native long matrixDone( long matrixInput );

	/**
	 * Frees a given matrix.
	 *
	 * @param matrix Pointer to the GraphBLAS matrix to be destroyed. This pointer
	 *               will no longer be valid to pass to other Native functions.
	 *
	 */
	// public static native void destroyMatrix( long matrix );

	/**
	 * Frees a given vector.
	 *
	 * @param vector Pointer to the GraphBLAS vector to be destroyed. This pointer
	 *               will no longer be valid to pass to other Native functions.
	 */
	public static native void destroyVector( long vector );

	/*
	 * Runs PageRank on a given matrix and returns its PageRank vector.
	 *
	 * @param[in] matrix The GraphBLAS matrix to run PageRank on.
	 * @param[in,out] vector On input: an initial approximation to the pagerank
	 *                       vector. On output: the computed pagerank vector.
	 *
	 * @returns A pointer to a GraphBLAS vector containing the PageRanks.
	 */
	//public static native long pagerank( long matrix, long vector );

	/**
	 * Runs a given algorithm on a given matrix file and returns a handle to its
	 * output vector.
	 *
	 * Supported algorithms are:
	 *  -# PAGERANK_GRB_IO
	 *
	 * @param instance A Spark GraphBLAS instance as returned by #begin.
	 * @param program  Which GraphBLAS program to execute.
	 * @param filename An absolute path to a matrix file. This file must be
	 *                 available on all nodes.
	 * @return A handle to an output vector.
	 *
	 * Output vectors are freed via a call to #destroyVector.
	 */
	public static native long execIO( long instance, int program, String filename );

	/**
	 * Returns the index of the maximum value in a vector.
	 *
	 * Ties will be broken arbitrarily.
	 *
	 * @param vector An output vector.
	 * @return The requested index.
	 */
	public static native long argmax( long vector );

	/**
	 * Returns the value corresponding to a given index in a vector.
	 *
	 * @param vector From which vector to retrieve an element of.
	 * @param index  Which index to return the value of.
	 *
	 * @return The element at the given index.
	 *
	 * If the vector entry at the given entry does not exist, 0 is returned.
	 */
	public static native double getValue( long vector, long index );


	public static native boolean enterSequence();

	public static native void exitSequence();

	public static native long getIterations();

	public static native long getTime();

	public static native long getOuterIterations();

};

