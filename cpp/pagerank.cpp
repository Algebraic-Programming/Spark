
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


constexpr unsigned iters = 5;
using duration_t = std::chrono::time_point<std::chrono::high_resolution_clock>::duration;
static duration_t pr_time = duration_t::zero();

unsigned get_pr_iterations() {
	return iters;
}

unsigned long get_pr_time() {
	return static_cast< unsigned long >(
		std::chrono::duration_cast<std::chrono::nanoseconds>( pr_time ).count()
	);
}

/**
 * Starts the GraphBLAS pagerank algorithm in #grb::algorithms by reading an
 * input file in parallel.
 *
 * @param[in]  data_in  The input structure. Contains the file name of the input
 *                      matrix.
 * @param[out] data_out The output pagerank vector.
 */
void grb_pagerank( const GrB_Input &data_in, GrB_Output &out ) {
	const size_t s = grb::spmd<>::pid();
	const size_t P = grb::spmd<>::nprocs();
	const size_t omP = grb::config::OMP::threads();
	assert( s < P );
	out.error_code = grb::PANIC;
	out.iterations = std::numeric_limits< size_t >::max();
	out.residual = std::numeric_limits< double >::infinity();
	out.pinnedVector = nullptr;
#ifdef FILE_LOGGING
	std::string fp = getenv("HOME");
	fp += "/graphblastest.txt";
	FILE * file = fopen( fp.c_str(), "a" );
	(void) fprintf( file,
		"This is process %d. I am BSP process %zd out of %zd. OpenMP uses %zd "
		"threads.\n",
		getpid(), s, P, omP );
	(void) fflush( file );

#endif
	assert( data_in.program == PAGERANK_GRB_IO );
#ifdef FILE_LOGGING
	(void) fprintf( file, "I will perform a PageRank using GraphBLAS I/O. Filename: %s\n", data_in.data );
	(void) fflush( file );
#endif
	try {
		grb::utils::MatrixFileReader< void > parser( data_in.data, 1 );

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
// 		if( grb::nnz( L ) != parser.nz() ) {
// #ifdef FILE_LOGGING
// 			(void) fprintf( file,
// 				"Number of nonzeroes in grb::Matrix (%zd) does not match that in "
// 				"file (%zd)!\n",
// 				grb::nnz( L ), parser.nz() );
// #endif
// 			out.error_code = grb::PANIC;
// 			return;
// 		}
		grb::Vector< double > pr( n ), workspace1( n ), workspace2( n ), workspace3( n );
#ifdef FILE_LOGGING
		(void) fprintf( file, "Output vector allocated, now passing to PageRank function\n" );
		(void) fflush( file );
#endif
		constexpr double tolerance = 0.0000001;
		constexpr unsigned maxIters = 80;
		out.error_code = grb::algorithms::simple_pagerank< grb::descriptors::no_operation >(
			pr, L,
			workspace1, workspace2, workspace3,
			0.85, tolerance, maxIters,
			&( out.iterations ), &( out.residual )
		);
		pr_time = duration_t::zero();
		for( unsigned i = 0; i < iters; i++ ) {
			grb::clear( pr );
			auto start = std::chrono::high_resolution_clock::now();
			out.error_code = grb::algorithms::simple_pagerank< grb::descriptors::no_operation >(
				pr, L,
				workspace1, workspace2, workspace3,
				0.85, tolerance, maxIters,
				&( out.iterations ), &( out.residual )
			);
			auto end = std::chrono::high_resolution_clock::now();
			pr_time += ( end - start );
		}

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
		// std::string fn = "/home/ascolari/Projects/ALP-Spark/pr-" + std::to_string(getpid()) + ".log";
		// std::ofstream myfile(fn);
		// myfile << "ciao" << std::endl;
		// myfile << "nonzero " << nnz(pr) << std::endl;
		out.pinnedVector = new grb::PinnedVector< double >( pr, grb::PARALLEL );
#ifdef FILE_LOGGING
		(void) fprintf( file, "iters = %zd, residual = %.10e\n", out.iterations, out.residual );
		(void) fprintf( file, "returning pinned vector @ %p. It contains %zd elements.\n", out.pinnedVector, out.pinnedVector->nonzeroes() );
#endif
	} catch (std::runtime_error e ){
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

