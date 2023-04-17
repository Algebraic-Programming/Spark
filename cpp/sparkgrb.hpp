
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

#ifndef _H_SPARKGRB
#define _H_SPARKGRB

#include <vector>

#include "graphblas.hpp"

#ifdef __DOXYGEN__
/**
 * Define this macro to enable logging into /tmp/graphblastest.txt. This file
 * will be written at every worker node.
 */
#define FILE_LOGGING
#endif

#define FILE_LOGGING

/** The persistent data that is passed around via Spark. */
typedef grb::Launcher< grb::EXEC_MODE::MANUAL > Persistent;

/** Persistent data used for streaming in pattern matrix data. */
typedef struct {
	size_t m;
	size_t n;
	std::pair< std::vector< size_t >, std::vector< size_t > > coordinates;
} MatrixUnderConstruction;

/** A list of all supported GraphBLAS subroutines / programs. */
enum SupportedPrograms { PAGERANK_GRB_IO };

/**
 * Generic input to GraphBLAS subroutines / programs.
 */
typedef struct {
	char data[ 1024 ];
	SupportedPrograms program;
} GrB_Input;

/**
 * Generic output struct of GraphBLAS subroutines / programs.
 */
typedef struct {
	enum grb::RC error_code;
	size_t iterations;
	double residual;
	grb::PinnedVector< double > * pinnedVector;
} GrB_Output;

void grb_pagerank( const GrB_Input &data_in, GrB_Output &out );

#endif

