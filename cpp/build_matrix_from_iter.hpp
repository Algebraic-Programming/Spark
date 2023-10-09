
#pragma once

#include <graphblas.hpp>

#include "sparkgrb.hpp"
#include "ingestion_data.hpp"

template< typename RowT, typename ColT > struct build_params {
	ingestion_data< RowT, ColT > &ingestion;
	std::size_t rows;
	std::size_t cols;
};

template< typename RowT, typename ColT, typename ValT > void
build_matrix_from_iterator(
	const build_params< RowT, ColT > &pars,
	grb::Matrix< ValT >* &ret
) {
	set_omp_threads();
	grb::Matrix< ValT > * const _ret = new grb::Matrix< ValT >( pars.rows, pars.cols );
	entry_iterator< RowT, ColT > start( pars.ingestion.get_begin() ),
		end( pars.ingestion.get_end() );

	const grb::RC rc = grb::buildMatrixUnique( *_ret, start, end, grb::PARALLEL );
	if( rc != grb::SUCCESS ) {
		throw std::runtime_error( "cannot build matrix" );
	}
	ret = _ret;
}
