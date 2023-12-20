
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
#include <vector>

#include "graphblas/algorithms/simple_pagerank.hpp"
#include "graphblas/utils/parser/MatrixFileReader.hpp"

#include "sparkgrb.hpp"

#include "pagerank.hpp"
#include "logger.h"


using duration_t = std::chrono::time_point<std::chrono::high_resolution_clock>::duration;


static std::vector< double > ms_times;

double* get_ms_times_pointer() {
	return ms_times.data();
}

size_t get_ms_times_size() {
	return ms_times.size();
}


static std::vector< unsigned > iterations;

unsigned* get_iterations_pointer() {
	return iterations.data();
}

size_t get_iterations_size() {
	return iterations.size();
}


void do_pagerank( const pagerank_input &data_in, pagerank_output &out ) {

	try {
		const std::size_t n = grb::nrows( *data_in.data );
		grb::Vector< double > pr( n ), workspace1( n ), workspace2( n ), workspace3( n );
		LOG( INFO, "Output vector allocated, now passing to PageRank function" );
		out.error_code = grb::algorithms::simple_pagerank< grb::descriptors::no_operation >(
			pr, *data_in.data,
			workspace1, workspace2, workspace3,
			data_in.alpha, data_in.tolerance, data_in.max_iterations,
			&( out.iterations ), &( out.residual )
		);

		if( out.error_code != grb::SUCCESS ) {
			LOG( ERROR, "error running PageRank warmup" );
			return;
		}

		// duration_t pr_time = duration_t::zero();
		ms_times.clear();
		iterations.clear();
		LOG( INFO, "available threads: %d", omp_get_max_threads() );
		LOG( INFO, "PageRank running for %u times", data_in.outer_iters );
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
			ms_times.push_back( std::chrono::duration_cast< std::chrono::duration< double, std::milli > >( end - start ).count() );
			iterations.push_back( out.iterations );

			LOG( INFO, "PageRank ran for %u iterations, %lf ms",
				iterations.back(), ms_times.back() );
		}

		if( out.error_code != grb::SUCCESS ) {
			std::string error_code = grb::toString( out.error_code );
			LOG( ERROR, "Call to PageRank failed with error code %s; ", error_code.c_str() );
			return;
		}
		LOG( INFO, "Call to PageRank successful; " );
		out.pinnedVector = new grb::PinnedVector< double >( pr, grb::PARALLEL );

		LOG( INFO, "iters = %zd, residual = %.10e", out.iterations, out.residual );
		LOG( INFO, "returning pinned vector @ %p. It contains %zd elements.", out.pinnedVector, out.pinnedVector->nonzeroes() );
	} catch( std::runtime_error &e ) {
		LOG( INFO, "Got exception: %s", e.what() );
		out.error_code = grb::PANIC;
	}
	LOG( INFO, "Exiting grb_program." );
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
	out.error_code = grb::PANIC;
	out.iterations = std::numeric_limits< size_t >::max();
	out.residual = std::numeric_limits< double >::infinity();
	out.pinnedVector = nullptr;

	LOG( INFO, "This is process %d. I am BSP process %lu out of %lu. OpenMP uses %d "
		"threads. My OMP_NUM_THREADS %s.", getpid(), grb::spmd<>::pid(),
		grb::spmd<>::nprocs(), omp_get_max_threads(), getenv( "OMP_NUM_THREADS" ) );

	LOG( INFO, "I will perform a PageRank using GraphBLAS I/O. Filename: %s", data_in.infile );

	try {
		grb::utils::MatrixFileReader< void > parser( data_in.infile, true );

		const size_t m = parser.m();
		const size_t n = parser.n();
		if( m != n ) {
			out.error_code = grb::ILLEGAL;
			return;
		}
		grb::Matrix< void > L( n, n );
		out.error_code = grb::buildMatrixUnique( L, parser.begin( grb::PARALLEL ), parser.end( grb::PARALLEL ), grb::PARALLEL );
		if( out.error_code != grb::SUCCESS ) {
			LOG( ERROR, "cannot read input using GraphBLAS I/O. Filename: %s", data_in.infile );
			return;
		}
		try {
			if( grb::nnz( L ) != parser.nz() ) {
				LOG( ERROR,
					"Error: number of nonzeroes in grb::Matrix (%zd) does not match that "
					"reported by the parser (%zd)", grb::nnz( L ), parser.nz()
				);
				out.error_code = grb::PANIC;
				return;
			}
		} catch( ... ) {
			LOG( WARNING,
				"exception caught during call involing parser.nz(). Assuming that "
				"this is cased by input matrix symmetry."
			);
		}
		pagerank_input final( data_in );
		final.data = &L;

		do_pagerank( final, out );

	} catch( std::runtime_error &e ){
		LOG( ERROR, "Got exception: %s", e.what() );
		out.error_code = grb::PANIC;
	}

	LOG( INFO, "Exiting grb_pagerank_from_file." );
	LOG_EXIT();
}
