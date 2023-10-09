
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

#include <string>
#include <stdlib.h>
#include <chrono>
#include <limits>

#include "graphblas/algorithms/simple_pagerank.hpp"
#include "graphblas/utils/parser/MatrixFileReader.hpp"

#include "sparkgrb.hpp"

#include "pagerank.hpp"


unsigned outer_iters = 5;
using duration_t = std::chrono::time_point<std::chrono::high_resolution_clock>::duration;
static duration_t pr_time = duration_t::zero();
size_t iterations;

size_t get_pr_inner_iterations() {
	return iterations;
}

unsigned long get_pr_time() {
	return static_cast< unsigned long >(
		std::chrono::duration_cast<std::chrono::nanoseconds>( pr_time ).count()
	);
}

void do_pagerank( const pagerank_input &data_in, pagerank_output &out ) {

		set_omp_threads();
	try {
		const std::size_t n = grb::nrows( *data_in.data );
		grb::Vector< double > pr( n ), workspace1( n ), workspace2( n ), workspace3( n );
#ifdef FILE_LOGGING
		(void) fprintf( file, "Output vector allocated, now passing to PageRank function\n" );
		(void) fflush( file );
#endif
		out.error_code = grb::algorithms::simple_pagerank< grb::descriptors::no_operation >(
			pr, *data_in.data,
			workspace1, workspace2, workspace3,
			/*0.85*/ data_in.alpha, data_in.tolerance, data_in.max_iterations,
			&( out.iterations ), &( out.residual )
		);
		pr_time = duration_t::zero();
		for( unsigned i = 0; i < data_in.outer_iters; i++ ) {
			grb::clear( pr );
			auto start = std::chrono::high_resolution_clock::now();
			out.error_code = grb::algorithms::simple_pagerank< grb::descriptors::no_operation >(
				pr, *data_in.data,
				workspace1, workspace2, workspace3,
				data_in.alpha, data_in.tolerance, data_in.max_iterations,
				&( out.iterations ), &( out.residual )
			);
			auto end = std::chrono::high_resolution_clock::now();
			pr_time += ( end - start );
		}
		iterations = out.iterations;

		if( out.error_code != grb::SUCCESS ) {
			std::string error_code = grb::toString( out.error_code );
#ifdef FILE_LOGGING
			(void) fprintf( file, "Call to PageRank failed with error code %s; ", error_code.c_str() );
#endif
			return;
		}
#ifdef FILE_LOGGING
		(void) fprintf( file, "Call to PageRank successful; " );
#endif
		out.pinnedVector = new grb::PinnedVector< double >( pr, grb::PARALLEL );
#ifdef FILE_LOGGING
		(void) fprintf( file, "iters = %zd, residual = %.10e\n", out.iterations, out.residual );
		(void) fprintf( file, "returning pinned vector @ %p. It contains %zd elements.\n", out.pinnedVector, out.pinnedVector->nonzeroes() );
#endif
	} catch( std::runtime_error &e ){
#ifdef FILE_LOGGING
		(void) fprintf( file, "Got exception: %s\n", e.what() );
		(void) fflush( file );
#endif
		out.error_code = grb::PANIC;
	}

#ifdef FILE_LOGGING
	(void) fprintf( file, "Exiting grb_program.\n" );
	(void) fclose( file );
#endif
}

/**
 * Starts the GraphBLAS pagerank algorithm in #grb::algorithms by reading an
 * input file in parallel.
 *
 * @param[in]  data_in  The input structure. Contains the file name of the input
 *                      matrix.
 * @param[out] data_out The output pagerank vector.
 */
void grb_pagerank_from_file( const pagerank_file_input &data_in, pagerank_output &out ) {
	set_omp_threads();
#if !defined NDEBUG || defined FILE_LOGGING
	const size_t s = grb::spmd<>::pid();
	const size_t P = grb::spmd<>::nprocs();
	assert( s < P );
#endif
	out.error_code = grb::PANIC;
	out.iterations = std::numeric_limits< size_t >::max();
	out.residual = std::numeric_limits< double >::infinity();
	out.pinnedVector = nullptr;
#ifdef FILE_LOGGING
	std::string fp = "/tmp/graphblastest.txt";
	FILE * file = fopen( fp.c_str(), "a" );
	#pragma omp parallel
	{
		#pragma omp single
		{
			const size_t omP = omp_get_num_threads();
			const char * envchk = getenv( "OMP_NUM_THREADS" );
			const char * prefix = "reads ";
			if( envchk == NULL ) { envchk = "not exist"; prefix = "does "; }
			(void) fprintf( file,
				"This is process %d. I am BSP process %zd out of %zd. OpenMP uses %zd "
				"threads. My OMP_NUM_THREADS %s %s.\n",
				getpid(), s, P, omP, prefix, envchk );
			(void) fflush( file );
		}
	}

	(void) fprintf( file, "I will perform a PageRank using GraphBLAS I/O. Filename: %s\n", data_in.data );
	(void) fflush( file );
#endif
	try {
		grb::utils::MatrixFileReader< void > parser( data_in.infile, true );

		const size_t m = parser.m();
		const size_t n = parser.n();
#ifdef FILE_LOGGING
		// (void) fprintf( file, "File parsed: input matrix is %zd by %zd, and contains %zd nonzeroes.\n", m, n, parser.nz() );
		// (void) fflush( file );
#endif
		if( m != n ) {
			out.error_code = grb::ILLEGAL;
			return;
		}
		grb::Matrix< void > L( n, n );
		out.error_code = grb::buildMatrixUnique( L, parser.begin( grb::PARALLEL ), parser.end( grb::PARALLEL ), grb::PARALLEL );
		if( out.error_code != grb::SUCCESS ) {
#ifdef FILE_LOGGING
			(void) fprintf( file, "Could not build matrix.\n" );
			(void) fflush( file );
#endif
			return;
		}
		try {
			if( grb::nnz( L ) != parser.nz() ) {
#ifdef FILE_LOGGING
				(void) fprintf( file,
					"Error: number of nonzeroes in grb::Matrix (%zd) does not match that "
					"reported by the parser (%zd)\n", grb::nnz( L ), parser.nz()
				);
#endif
				out.error_code = grb::PANIC;
				return;
			}
		} catch( ... ) {
#ifdef FILE_LOGGING
			(void) fprintf( file,
				"Warning: exception caught during call involing parser.nz(). Assuming that "
				"this is cased by input matrix symmetry.\n"
			);
#endif
		}
		pagerank_input final( data_in );
		final.data = &L;

		do_pagerank( final, out );

	} catch( std::runtime_error &e ){
#ifdef FILE_LOGGING
		(void) fprintf( file, "Got exception: %s\n", e.what() );
		(void) fflush( file );
#endif
		out.error_code = grb::PANIC;
	}

#ifdef FILE_LOGGING
	(void) fprintf( file, "Exiting grb_pagerank_from_file.\n" );
	(void) fclose( file );
#endif
}
