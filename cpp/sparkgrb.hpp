
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
#pragma once

#include <vector>
#include <limits>

#include "graphblas.hpp"

#ifdef __DOXYGEN__
/**
 * Define this macro to enable logging into /tmp/graphblastest.txt. This file
 * will be written at every worker node.
 */
#define FILE_LOGGING
#endif

// #define FILE_LOGGING

/** The persistent data that is passed around via Spark. */
typedef grb::Launcher< grb::EXEC_MODE::MANUAL > Persistent;

/** Persistent data used for streaming in pattern matrix data. */
// typedef struct {
// 	size_t m;
// 	size_t n;
// 	std::vector< size_t > rows;
// 	std::vector< size_t > cols;
// } MatrixUnderConstruction;

/** A list of all supported GraphBLAS subroutines / programs. */
enum SupportedPrograms { PAGERANK_GRB_IO };

struct pagerank_input {
	grb::Matrix< void > * data = nullptr;
	double tolerance = 0.0000001;
	unsigned max_iterations = 80;
	unsigned outer_iters = 5;
	double alpha = 0.85;

	pagerank_input() = default;

	pagerank_input( const pagerank_input & ) = default;
};

/**
 * Generic input to GraphBLAS subroutines / programs.
 */
struct pagerank_file_input : pagerank_input {

	static constexpr unsigned STR_SIZE = 1023;

	char infile[ STR_SIZE + 1 ] = {'\0'};

	pagerank_file_input() = default;
};

/**
 * Generic output struct of GraphBLAS subroutines / programs.
 */
struct pagerank_output {
	enum grb::RC error_code = grb::SUCCESS;
	size_t iterations = 0;
	double residual = std::numeric_limits< double >::max();
	grb::PinnedVector< double > * pinnedVector = nullptr;

	pagerank_output() = default;

	pagerank_output( const pagerank_output & ) = delete;
};

void set_omp_threads();

void do_pagerank( const pagerank_input &, pagerank_output & );

void grb_pagerank_from_file( const pagerank_file_input &, pagerank_output & );
