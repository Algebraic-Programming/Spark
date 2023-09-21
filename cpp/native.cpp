
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

#include "com_huawei_graphblas_Native.h"
#include "graphblas.hpp"
#include "sparkgrb.hpp"
#include "pagerank.hpp"

#include <string>

#include <assert.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <fstream>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <stdexcept>
#include <cstdio>
#include <unordered_map>
#include <cstdlib>
#include <limits>
#include <malloc.h>

#include <sys/types.h>
#include <fstream>

static Persistent * grb_instance = nullptr;

// do not initialize MPI when loading the library
const int LPF_MPI_AUTO_INITIALIZE = 0;

JNIEXPORT jlong JNICALL Java_com_huawei_graphblas_Native_execIO( JNIEnv * env, jclass classDef, jlong instance, jint program, jstring filename ) {
	(void) classDef;
	(void) instance;

	const char * const cfn = env->GetStringUTFChars( filename, NULL );

	// parse arguments
#ifdef FILE_LOGGING
	std::string fp = getenv("HOME");
	fp += "/graphblastest.txt";
	FILE * file = fopen( fp.c_str(), "a" );
#endif
	// Persistent * const launcher_p = reinterpret_cast< Persistent * >( instance );
	if( grb_instance == nullptr ) {
		throw std::runtime_error( "instance not valid" );
	}
	Persistent * const launcher_p = grb_instance;
#ifdef FILE_LOGGING
	(void)fprintf( file,
		"I am process %d and I have been asked to perform a graphBLAS program with "
		"graphBLAS-managed I/O. The matrix file I will read is at %s. The "
		"GraphBLAS launcher is at %p.\n",
		getpid(), cfn, launcher_p );
#endif

	// prepare input
	GrB_Input in;
	size_t cfn_size = strlen( cfn );
	if( cfn_size > 1023 ) {
		cfn_size = 1023;
	}
	strncpy( &( in.data[ 0 ] ), cfn, 1023 );
	in.data[ 1023 ] = '\0';
	in.program = PAGERANK_GRB_IO;
	env->ReleaseStringUTFChars( filename, cfn );

	// prepare output
	GrB_Output out;
	out.error_code = grb::SUCCESS;
	out.iterations = 0;
	out.residual = 0.0;

#ifdef FILE_LOGGING
	(void)fprintf( file,
		"Input and output structs have been initialised, now passing to GraphBLAS "
		"program...\n" );
	(void)fflush( file );
#endif

	// execute
	if( program == 0 ) {
		launcher_p->exec( &grb_pagerank, in, out, true );
#ifdef FILE_LOGGING
		std::string error = grb::toString( out.error_code );
		(void)fprintf( file, "Error code returned: %s\n", error.c_str() );
		(void)fprintf( file, "Number of iterations: %zd\n", out.iterations );
		(void)fprintf( file, "Final residual: %lf\n", out.residual );
#endif
	} else {
#ifdef FILE_LOGGING
		(void)fprintf( file, "Unknown program requested: %d. Ignoring call.\n", program );
#endif
	}

#ifdef FILE_LOGGING
	(void)fprintf( file,
		"Exiting program with GraphBLAS managed IO; returning output vector at "
		"%p.\n",
		out.pinnedVector );
	(void)fclose( file );
#endif

	return reinterpret_cast< long >( out.pinnedVector );
}

JNIEXPORT jlong JNICALL Java_com_huawei_graphblas_Native_start( JNIEnv * env, jclass classDef, jstring hostname, jint pid, jint P, jint threads ) {
	(void) env;
	(void) classDef;
#ifdef FILE_LOGGING
	FILE * file = fopen( "/tmp/graphblastest.txt", "a" );
	assert( file != NULL );
#endif
	char num_threads_env_var[ 16 ];
	sprintf( num_threads_env_var, "%d", (int)threads );
	setenv( "OMP_NUM_THREADS", num_threads_env_var, 1 ); // workaround to tell OMP the number of threads from Spark
	const char * const hostname_c = env->GetStringUTFChars( hostname, NULL );
	assert( hostname_c != NULL );
#ifdef FILE_LOGGING
	(void)fprintf( file,
		"I am process %d. I am about to create a grb::Launcher in manual mode. The "
		"hostname string I am passing to bsp_mpi_initialize_over_tcp is %s, and I "
		"am hardcoded to try port 7177. My LPF ID is %d, and the expected number "
		"of LPF processes is %d\n",
		getpid(), hostname_c, pid, P );
	(void)fflush( file );
#endif
	std::string hostname_str = hostname_c;
	env->ReleaseStringUTFChars( hostname, hostname_c );
	Persistent * const ret = new Persistent( pid, P, hostname_str, "7177", false );
	grb_instance = ret;
	assert( ret != NULL );
#ifdef FILE_LOGGING
	// do some logging
	(void)fprintf( file, "Launcher instance @ %p\n", ret );
	(void)fclose( file );
#endif
	return reinterpret_cast< long >( ret );
}

JNIEXPORT void JNICALL Java_com_huawei_graphblas_Native_end( JNIEnv * env, jclass classDef, jlong data ) {
	(void) env;
	(void) classDef;
	(void) data;
#ifdef FILE_LOGGING
	FILE * file = fopen( "/tmp/graphblastest.txt", "a" );
#endif
	if( grb_instance == nullptr ) {
		throw std::runtime_error( "instance not valid" );
	}
	Persistent * const launcher_p = grb_instance;
#ifdef FILE_LOGGING
	(void) fprintf( file, "I am process %d. I am about to delete the launcher at %p... ", getpid(), launcher_p );
	(void) fflush( file );
#endif
	assert( launcher_p != NULL );
	delete launcher_p;
#ifdef FILE_LOGGING
	(void) fprintf( file, "done!\n" );
	(void) fclose( file );
#endif
}







// JNIEXPORT jlong JNICALL Java_com_huawei_graphblas_Native_createMatrix(
//     JNIEnv * env, jclass classDef,
//     jint pid, jint P,
//     jstring path
// ) {
//     if( path == NULL ) {
//         return 0;
//     }
//     jsize strlen = env->GetStringLength( path );
//     if( strlen == 0 ) {
//         return 0;
//     }
//     const char * const cfn = env->GetStringUTFChars( path, NULL );
//     assert( cfn != NULL );
//     const std::string fn = cfn;
//     grb::utils::MatrixFileReader<void> reader = grb::utils::MatrixFileReader<void>( std::string(fn) );
//     env->ReleaseStringUTFChars( path, cfn );
//     if( reader.m() == 0 || reader.n() == 0 ) {
//         return 0;
//     }
//     grb::Matrix<void> *ret = new grb::Matrix<void>( reader.m(), reader.n() );
//     assert( ret != NULL );
//     const grb::RC rc = grb::buildMatrixUnique( *ret, reader.cbegin(), reader.cend(), SEQUENTIAL );
//     assert( rc == SUCCESS );
//     return reinterpret_cast< jlong >(ret);
// }


std::mutex ingestion_mutex;

struct ingestion_data {
	std::atomic_size_t length;
	std::unordered_map< int, std::size_t > index_length;

	ingestion_data() : length( 0UL ) {}
};

struct ingestion_data * ingestion = nullptr;


JNIEXPORT jlong JNICALL Java_com_huawei_graphblas_Native_addDataSeries(
    JNIEnv *, jclass,
    jint index, jlong length
) {
	const int id = static_cast< int >( index );
	const std::size_t len = static_cast< std::size_t >( length );

	std::lock_guard<std::mutex> lock( ingestion_mutex );

	if( ingestion == nullptr ) {
		ingestion = new ingestion_data;
	}
	if( ingestion->index_length.find( id ) != ingestion->index_length.cend() ) {
		printf( "index %d already present\n", id );
		return -1;
	}
	std::size_t newLength = ingestion->length += len;
	std::size_t oldLength = newLength - len;
	printf( "inserting index %d\n", id );
	ingestion->index_length.emplace( static_cast< int >( id ),
		oldLength );
	return static_cast< jlong >( oldLength );
}

#pragma pack(1)
struct matrix_entry {
	std::size_t row;
	std::size_t col;
};

struct matrix_entry * entries = nullptr;


JNIEXPORT void JNICALL Java_com_huawei_graphblas_Native_allocateIngestionMemory(
    JNIEnv *, jclass
) {
	std::size_t size = ingestion->length * sizeof( matrix_entry );
	entries = ( matrix_entry * )memalign( sizeof( matrix_entry ), size );
	printf( "allocating size %lu at address %p\n", size, entries );
}

JNIEXPORT jlong JNICALL Java_com_huawei_graphblas_Native_getOffset(
    JNIEnv *, jclass
) {
	return static_cast< jlong >( sizeof( std::size_t ) );
}



JNIEXPORT jlong JNICALL Java_com_huawei_graphblas_Native_getIndexBaseAddress(
    JNIEnv *, jclass,
    jint index
) {
	const int id = static_cast< int >( index );
	typename std::unordered_map< int, std::size_t >::const_iterator el =
		ingestion->index_length.find( id );
	if( el == ingestion->index_length.cend() ) {
		printf( "index %d is absent\n", id );
		throw std::runtime_error( "index " + std::to_string( index ) + " not present" );
		// return static_cast< jlong >( std::numeric_limits< long >::max() );
	}
	return reinterpret_cast< jlong >( entries + el->second );
}

JNIEXPORT void JNICALL Java_com_huawei_graphblas_Native_cleanIngestionData(
    JNIEnv *, jclass
) {
	delete ingestion;
	ingestion = nullptr;
	free( entries );
	entries = nullptr;
}


/*
MatrixUnderConstruction * ret = nullptr;

JNIEXPORT jlong JNICALL Java_com_huawei_graphblas_Native_matrixInput(
    JNIEnv * env, jclass classDef,
    jint nrows, jint ncols
) {
    ret = new MatrixUnderConstruction();
    assert( ret != NULL );
    ret->m = nrows;
    ret->n = ncols;
    return reinterpret_cast< jlong >(ret);
}

JNIEXPORT void JNICALL Java_com_huawei_graphblas_Native_matrixAddRow(
    JNIEnv * env, jclass classDef,
    jlong matrixInput,
    jlong row, jlongArray colind
) {
    assert( matrixInput != 0 );
    MatrixUnderConstruction &matrix = *reinterpret_cast< MatrixUnderConstruction* >(matrixInput);
    if( matrix.m == 0 || matrix.n == 0 ) {
        return;
    }
    assert( row < matrix.m );
    const jint nonzeroes = env->GetArrayLength( colind );
    if( nonzeroes == 0 ) {
        return;
    }
    jlong * array = static_cast< jlong* >(env->GetPrimitiveArrayCritical( colind, NULL ));
    assert( array != NULL );
    for( size_t i = 0; i < nonzeroes; ++i ) {
        matrix.rows.push_back( row );
        matrix.cols.push_back( array[i] );
    }
    env->ReleasePrimitiveArrayCritical( colind, array, JNI_ABORT );
    return;
}

JNIEXPORT jlong JNICALL Java_com_huawei_graphblas_Native_matrixDone(
    JNIEnv * env, jclass classDef,
    jlong matrixInput
) {
    typedef std::vector<size_t>::const_iterator SubIterator;
    MatrixUnderConstruction &matrix = *reinterpret_cast< MatrixUnderConstruction* >(matrixInput);
    grb::Matrix<void> * const ret = new grb::Matrix<void>( matrix.m, matrix.n );
    assert( ret != NULL );
    auto start = grb::utils::SynchronizedNonzeroIterator< size_t, size_t, void, SubIterator, SubIterator, void >(
        matrix.rows.cbegin(), matrix.cols.cbegin()
    );
    const auto end = grb::utils::SynchronizedNonzeroIterator< size_t, size_t, void, SubIterator, SubIterator, void >(
        matrix.rows.cend(), matrix.cols.cend()
    );
    const grb::RC rc = grb::buildMatrixUnique( *ret, start, end, PARALLEL );
    assert( rc == SUCCESS );
    delete &matrix;
    return reinterpret_cast< jlong >(ret);
}

JNIEXPORT void JNICALL Java_com_huawei_graphblas_Native_destroyMatrix(
    JNIEnv * env, jclass classDef,
    jlong matrix
) {
    delete reinterpret_cast< grb::Matrix<void>* >(matrix);
}
*/








JNIEXPORT void JNICALL Java_com_huawei_graphblas_Native_destroyVector( JNIEnv * env, jclass classDef, jlong vector ) {
	(void) env;
	(void) classDef;

	grb::PinnedVector< double > * pointer = reinterpret_cast< grb::PinnedVector< double > * >( vector );
#ifdef FILE_LOGGING
	FILE * file = fopen( "/tmp/graphblastest.txt", "a" );
	(void) fprintf( file, "About to delete the PinnedVector at %p...", pointer );
	(void) fflush( file );
#endif
	delete pointer;
#ifdef FILE_LOGGING
	(void) fprintf( file, "done!\n" );
	(void) fclose( file );
#endif
}

JNIEXPORT jlong JNICALL Java_com_huawei_graphblas_Native_argmax( JNIEnv * env, jclass classDef, jlong vector ) {
	(void) env;
	(void) classDef;

	grb::PinnedVector< double > * pointer = reinterpret_cast< grb::PinnedVector< double > * >( vector );
#ifdef FILE_LOGGING
	FILE * file = fopen( "/tmp/graphblastest.txt", "a" );
	(void) fprintf( file, "Argmax called on the PinnedVector at %p...", pointer );
	(void) fflush( file );
#endif
	const size_t nnz = pointer->nonzeroes();
	// sleep(120);

	if( nnz == 0 ) {
		return static_cast< jlong >( -1 );
	}
	size_t curmaxi;
	if( nnz == 0 ) {
		curmaxi = static_cast< size_t >( -1 );
	} else {
		curmaxi = pointer->getNonzeroIndex( 0 );
		double curmax = pointer->getNonzeroValue( 0 );
		for( size_t i = 1; i < nnz; ++i ) {
			const size_t index = pointer->getNonzeroIndex( i );
			const double curval = pointer->getNonzeroValue( i );
			if( curval > curmax ) {
				curmaxi = index;
			}
		}
	}
#ifdef FILE_LOGGING
	(void) fprintf( file, "returning %zd.\n", curmaxi );
	(void) fclose( file );
#endif
	return static_cast< jlong >( curmaxi );
}

JNIEXPORT jdouble JNICALL Java_com_huawei_graphblas_Native_getValue( JNIEnv * env, jclass classDef, jlong vector, jlong index ) {
	(void) env;
	(void) classDef;

	grb::PinnedVector< double > * pointer = reinterpret_cast< grb::PinnedVector< double > * >( vector );
#ifdef FILE_LOGGING
	FILE * file = fopen( "/tmp/graphblastest.txt", "a" );
	(void) fprintf( file, "getValue called on the PinnedVector at %p with index %zd...\n", pointer, index );
	(void) fprintf( file, "Warning: in recent ALP/GraphBLAS distributions, a call to this function scans all nonzeroes in the given vector!\n" );
	(void) fflush( file );
#else
	(void) fprintf( stderr, "Warning: in recent ALP/GraphBLAS distributions, a call to this function scans all nonzeroes in the given vector!\n" );
#endif
	double ret = 0;
	bool found = false;
	for( size_t i = 0; i < pointer->nonzeroes(); ++i ) {
		assert( pointer->getNonzeroIndex <= std::numeric_limits< jlong >::max() );
		if( pointer->getNonzeroIndex( i ) == static_cast< size_t >(index) ) {
			ret = pointer->getNonzeroValue( i );
			found = true;
			break;
		}
	}
#ifdef FILE_LOGGING
	if( !found ) {
		(void) fprintf( file, "Error: requested nonzero value not found, returning zero\n" );
	}
	(void) fprintf( file, "returning %lf.\n", ret );
	(void) fclose( file );
#else
	if( !found ) {
		(void) fprintf( stderr, "Error: requested nonzero value not found, returning zero\n" );
	}
#endif
	return static_cast< jdouble >( ret );
}


static std::atomic_bool already_initialized(false);
// static std::mutex sequence_mutex;

JNIEXPORT jboolean JNICALL Java_com_huawei_graphblas_Native_enterSequence(
	JNIEnv * env, jclass classDef
) {
	(void) env; (void) classDef;
	bool f = false;
	const bool initializer = already_initialized.compare_exchange_strong( f, true );
	return static_cast< jboolean >( initializer );
}

JNIEXPORT void JNICALL Java_com_huawei_graphblas_Native_exitSequence(
	JNIEnv * env, jclass classDef
) {
	(void) env;
	(void) classDef;

	already_initialized.store( false );
}

JNIEXPORT jlong JNICALL Java_com_huawei_graphblas_Native_getIterations(JNIEnv *, jclass) {
	return static_cast< jlong >( get_pr_inner_iterations() );
}

JNIEXPORT jlong JNICALL Java_com_huawei_graphblas_Native_getTime(JNIEnv *, jclass) {
	return static_cast< jlong >( get_pr_time() );
}

JNIEXPORT jlong JNICALL Java_com_huawei_graphblas_Native_getOuterIterations(JNIEnv *, jclass) {
	return static_cast< jlong >( get_pr_outer_iterations() );
}

