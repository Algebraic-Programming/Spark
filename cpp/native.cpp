
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
#include <graphblas.hpp>
#include "sparkgrb.hpp"
#include "pagerank.hpp"

#include <string>

#include <assert.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <fstream>
#include <atomic>
#include <condition_variable>
#include <stdexcept>
#include <cstdio>
#include <unordered_map>
#include <cstdlib>
#include <limits>
#include <malloc.h>

#include <sys/types.h>
#include <fstream>

#include "matrix_entry.hpp"
#include "ingestion_data.hpp"
#include "entry_iterator.hpp"
#include "build_matrix_from_iter.hpp"

#include "logger.h"

static Persistent * grb_instance = nullptr;

// do not initialize MPI when loading the library
const int LPF_MPI_AUTO_INITIALIZE = 0;

JNIEXPORT jlong JNICALL Java_com_huawei_graphblas_Native_start(
	JNIEnv *env,
	jclass,
	jstring hostname,
	jint pid,
	jint P
) {
	LOG_INIT();
	const char * const hostname_c = env->GetStringUTFChars( hostname, NULL );
	assert( hostname_c != NULL );
	LOG( INFO,
		"I am process %d. I am about to create a grb::Launcher in manual mode. The "
		"hostname string I am passing to bsp_mpi_initialize_over_tcp is %s, and I "
		"am hardcoded to try port 7177. My LPF ID is %d, and the expected number "
		"of LPF processes is %d",
		getpid(), hostname_c, pid, P );
	std::string hostname_str = hostname_c;
	env->ReleaseStringUTFChars( hostname, hostname_c );

	LOG( INFO, "connecting to host: %s", hostname_str.c_str() );

	Persistent * const ret = new Persistent( pid, P, hostname_str, "7177", false );
	grb::utils::ignoreNonExistantId = true;
	grb_instance = ret;
	assert( ret != NULL );
	return reinterpret_cast< long >( ret );
}

JNIEXPORT void JNICALL Java_com_huawei_graphblas_Native_end( JNIEnv *, jclass, jlong ) {
	if( grb_instance == nullptr ) {
		throw std::runtime_error( "instance not valid" );
	}
	Persistent * const launcher_p = grb_instance;
	LOG( INFO, "I am process %d. I am about to delete the launcher at %p... ", getpid(), launcher_p );
	assert( launcher_p != NULL );
	delete launcher_p;
	LOG( INFO, "done!" );
}

JNIEXPORT jlong JNICALL Java_com_huawei_graphblas_Native_pagerankFromFile(
		JNIEnv * env,
		jclass,
		jstring filename,
		jint maxPageRankIteration,
		jdouble tolerance,
		jint numExperiments
) {
	const char * const cfn = env->GetStringUTFChars( filename, NULL );

	// parse arguments
	// Persistent * const launcher_p = reinterpret_cast< Persistent * >( instance );
	if( grb_instance == nullptr ) {
		throw std::runtime_error( "instance not valid" );
	}
	Persistent * const launcher_p = grb_instance;
	LOG( INFO,
		"I am process %d and I have been asked to perform a graphBLAS program with "
		"graphBLAS-managed I/O. The matrix file I will read is at %s. The "
		"GraphBLAS launcher is at %p.",
		getpid(), cfn, launcher_p );

	// prepare input
	pagerank_file_input in;
	in.tolerance = static_cast< double >( tolerance );
	in.max_iterations = static_cast< unsigned >( maxPageRankIteration );
	in.outer_iters = static_cast< unsigned >( numExperiments );
	in.alpha = 0.85;
	size_t cfn_size = strlen( cfn );
	if( cfn_size > pagerank_file_input::STR_SIZE ) {
		cfn_size = pagerank_file_input::STR_SIZE;
		LOG( ERROR, "input string is too long." );
		env->ReleaseStringUTFChars( filename, cfn );
		return 0L;
	}
	strncpy( in.infile, cfn, pagerank_file_input::STR_SIZE );
	env->ReleaseStringUTFChars( filename, cfn );
	in.infile[ pagerank_file_input::STR_SIZE ] = '\0';

	// prepare output
	pagerank_output out;
	out.error_code = grb::SUCCESS;
	out.iterations = 0;
	out.residual = 0.0;

	LOG( INFO,
		"Input and output structs have been initialised, now passing to GraphBLAS "
		"program..." );

	// execute
	launcher_p->exec( &grb_pagerank_from_file, in, out, false );
	std::string error = grb::toString( out.error_code );
	LOG( INFO, "Error code returned: %s", error.c_str() );
	LOG( INFO, "Number of iterations: %lu", out.iterations );
	LOG( INFO, "Final residual: %lf", out.residual );

	LOG( INFO,
		"Exiting program with GraphBLAS managed IO; returning output vector at "
		"%p.",
		out.pinnedVector );

	return reinterpret_cast< long >( out.pinnedVector );
}

JNIEXPORT jlong JNICALL Java_com_huawei_graphblas_Native_pagerankFromGrbMatrix(
		JNIEnv *,
		jclass,
		jlong matrix,
		jint maxPageRankIteration,
		jdouble tolerance,
		jint numExperiments
) {
	if( grb_instance == nullptr ) {
		throw std::runtime_error( "instance not valid" );
	}
	Persistent * const launcher_p = grb_instance;
	grb::Matrix< void > * mat = reinterpret_cast< grb::Matrix< void > * >( matrix ) ;
	pagerank_input in;
	in.data = mat;
	in.tolerance = static_cast< double >( tolerance );
	in.max_iterations = static_cast< unsigned >( maxPageRankIteration );
	in.outer_iters = static_cast< unsigned >( numExperiments );
	in.alpha = 0.85;

	pagerank_output out;
	LOG( INFO, "invoking do_pagerank");

	launcher_p->exec( &do_pagerank, in, out, false );
	if( out.pinnedVector == nullptr ) {
		throw std::runtime_error( "could not run pagerank" );
	}
	LOG( INFO, "do_pagerank successful!");
	return reinterpret_cast< jlong >( out.pinnedVector );
}

JNIEXPORT jlong JNICALL Java_com_huawei_graphblas_Native_addDataSeries(
    JNIEnv *, jclass,
    jint index, jlong length
) {
	const int id = static_cast< int >( index );
	const std::size_t len = static_cast< std::size_t >( length );

	ingestion_data< std::size_t, std::size_t > & ingestion =
		ingestion_data< std::size_t, std::size_t >::get_instance();
	std::size_t oldLength = ingestion.add_index( id, len );

	return static_cast< jlong >( oldLength );
}


JNIEXPORT jlong JNICALL Java_com_huawei_graphblas_Native_allocateIngestionMemory(
    JNIEnv *, jclass
) {
	ingestion_data< std::size_t, std::size_t > & ingestion =
		ingestion_data< std::size_t, std::size_t >::get_instance();
	ingestion.allocate_entries();
	LOG( INFO, "allocating entries");
	return reinterpret_cast< jlong >( &ingestion );
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
	ingestion_data< std::size_t, std::size_t > & ingestion =
		ingestion_data< std::size_t, std::size_t >::get_instance();
	const int id = static_cast< int >( index );
	return reinterpret_cast< jlong >( ingestion.get_index_storage( id ) );
}

JNIEXPORT jlong JNICALL Java_com_huawei_graphblas_Native_ingestIntoMatrix(
    JNIEnv *, jclass,
	jlong rows, jlong cols
) {
	Persistent * const launcher_p = grb_instance;

	std::size_t r = static_cast< std::size_t >( rows ), c = static_cast< std::size_t >( cols );

	ingestion_data< std::size_t, std::size_t > & ingestion =
		ingestion_data< std::size_t, std::size_t >::get_instance();

	grb::Matrix< void >* ret = nullptr;

	build_params< std::size_t, std::size_t > input{ ingestion, r, c };

	LOG( INFO, "invoking matrix construction" );

	grb::RC rc = launcher_p->exec( build_matrix_from_iterator< std::size_t, std::size_t, void >, input, ret, false );

	if( rc != grb::SUCCESS ) {
		throw std::runtime_error( "cannot invoke matrix construction" );
	}
	if( ret == nullptr ) {
		throw std::runtime_error( "matrix construction failed" );
	}
	LOG( INFO, "matrix constructed" );

	return reinterpret_cast< jlong >( ret );
}

JNIEXPORT void JNICALL Java_com_huawei_graphblas_Native_cleanIngestionData(
    JNIEnv *, jclass
) {
	ingestion_data< std::size_t, std::size_t >::clear_instance();
}


template< typename ValT >
void delete_matrix( grb::Matrix< ValT >* const &mat, grb::RC &out ) {
	try {
		delete mat;
	} catch ( ... ) {
		out = grb::PANIC;
		return;
	}
	out = grb::SUCCESS;
}

JNIEXPORT void JNICALL Java_com_huawei_graphblas_Native_destroyMatrix(
    JNIEnv *, jclass,
    jlong matrix
) {
	if( grb_instance == nullptr ) {
		throw std::runtime_error( "instance not valid" );
	}
	if( matrix == 0L ) {
		LOG( ERROR, "wrong pointer passed");
		return;
	}
	Persistent * const launcher_p = grb_instance;
    grb::Matrix< void >* p = reinterpret_cast< grb::Matrix< void >* >(matrix);
	grb::RC out = grb::PANIC;
	LOG( INFO, "invoking matrix destruction" );
	grb::RC rc = launcher_p->exec( delete_matrix, p, out, false );
	if( out != grb::SUCCESS || rc != grb::SUCCESS ) {
		throw std::runtime_error( "cannot invoke matrix deletion" );
	}
	LOG( INFO, "matrix destroyed" );
}

JNIEXPORT void JNICALL Java_com_huawei_graphblas_Native_destroyVector( JNIEnv *, jclass, jlong vector ) {
	if( vector == 0L ) {
		LOG( ERROR, "wrong pointer passed");
		return;
	}

	grb::PinnedVector< double > * pointer = reinterpret_cast< grb::PinnedVector< double > * >( vector );
	LOG( INFO, "invoking vector destruction" );
	delete pointer;
	LOG( INFO, "vector destroyed" );
}

JNIEXPORT jlong JNICALL Java_com_huawei_graphblas_Native_argmax( JNIEnv * env, jclass classDef, jlong vector ) {
	(void) env;
	(void) classDef;

	grb::PinnedVector< double > * pointer = reinterpret_cast< grb::PinnedVector< double > * >( vector );
	LOG( INFO, "Argmax called on the PinnedVector at %p...", pointer );
	const size_t nnz = pointer->nonzeroes();

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
	LOG( INFO, "returning %zd.", curmaxi );
	return static_cast< jlong >( curmaxi );
}

JNIEXPORT jdouble JNICALL Java_com_huawei_graphblas_Native_getValue( JNIEnv * env, jclass classDef, jlong vector, jlong index ) {
	(void) env;
	(void) classDef;

	grb::PinnedVector< double > * pointer = reinterpret_cast< grb::PinnedVector< double > * >( vector );
	LOG( INFO, "getValue called on the PinnedVector at %p with index %zd...", pointer, index );
	LOG( WARNING, "in recent ALP/GraphBLAS distributions,"
		" a call to this function scans all nonzeroes in the given vector!" );
	double ret = 0;
	bool found = false;
	for( size_t i = 0; i < pointer->nonzeroes(); ++i ) {
		assert( pointer->getNonzeroIndex( i ) <= std::numeric_limits< size_t >::max() );
		if( pointer->getNonzeroIndex( i ) == static_cast< size_t >(index) ) {
			ret = pointer->getNonzeroValue( i );
			found = true;
			break;
		}
	}
	LOG( INFO, "returning %lf.", ret );
	if( !found ) {
		LOG( ERROR, "requested nonzero value not found, returning zero" );
	}
	return static_cast< jdouble >( ret );
}

static std::atomic_bool already_initialized(false);

JNIEXPORT jboolean JNICALL Java_com_huawei_graphblas_Native_enterSequence(
	JNIEnv * env, jclass classDef
) {
	(void) env; (void) classDef;
	bool f = false;
	const bool initializer = already_initialized.compare_exchange_strong( f, true );
	return static_cast< jboolean >( initializer );
}

JNIEXPORT void JNICALL Java_com_huawei_graphblas_Native_exitSequence(
	JNIEnv *, jclass
) {
	already_initialized.store( false );
}


double* get_ms_times_pointer();
JNIEXPORT jlong JNICALL Java_com_huawei_graphblas_Native_getMsTimesPointer(JNIEnv *, jclass) {
	return reinterpret_cast< jlong >( get_ms_times_pointer() );
}

size_t get_ms_times_size();
JNIEXPORT jlong JNICALL Java_com_huawei_graphblas_Native_getMsTimesSize(JNIEnv *, jclass) {
	return static_cast< jlong >( get_ms_times_size() );
}

unsigned* get_iterations_pointer();
JNIEXPORT jlong JNICALL Java_com_huawei_graphblas_Native_getIterationsPointer(JNIEnv *, jclass) {
	return reinterpret_cast< jlong >( get_iterations_pointer() );
}

size_t get_iterations_size();
JNIEXPORT jlong JNICALL Java_com_huawei_graphblas_Native_getIterationsSize(JNIEnv *, jclass) {
	return static_cast< jlong >( get_iterations_size() );
}
