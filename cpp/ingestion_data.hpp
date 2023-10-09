
#pragma once

#include <stdlib.h>

#include <atomic>
#include <unordered_map>
#include <mutex>
#include <limits>
#include <stdexcept>
#include <string>

#include "matrix_entry.hpp"

template< typename RowT, typename ColT >
class ingestion_data {

	static std::mutex ingestion_mutex;
	static ingestion_data< RowT, ColT > * volatile ingestion;

	std::atomic_size_t length;
	std::unordered_map< int, std::size_t > index_length;
	matrix_entry< RowT, ColT > *entries;

	ingestion_data() : length( 0UL ), entries( nullptr ) {}

	~ingestion_data() {
		if( entries != nullptr ) {
			delete entries;
		}
	}

public:

	using matrix_entry_t = matrix_entry< RowT, ColT >;
	using map_iter_t = typename std::unordered_map< int, std::size_t >::const_iterator;

	// produces singleton
	static ingestion_data< RowT, ColT > & get_instance() {

		if( ingestion != nullptr ) {
			return *ingestion;
		}
		{
			std::lock_guard<std::mutex> lock( ingestion_mutex );
			if( ingestion == nullptr ) {
				ingestion = new ingestion_data< RowT, ColT >;
			}
		}
		return *ingestion;
	}

	static void clear_instance() {
		std::lock_guard<std::mutex> lock( ingestion_mutex );
		if( ingestion != nullptr ) {
			delete ingestion;
		}
	}

	ingestion_data( const ingestion_data< RowT, ColT > & ) = delete;

	ingestion_data( ingestion_data< RowT, ColT > && ) = delete;

	void allocate_entries() {
		std::size_t size = length * sizeof( matrix_entry< RowT, ColT > );
		entries = ( matrix_entry< RowT, ColT > * )aligned_alloc( sizeof( matrix_entry< RowT, ColT > ), size );
	}

	std::size_t get_length() const {
		return length;
	}

	matrix_entry< RowT, ColT > *get_entries() {
		return entries;
	}

	std::size_t add_index( int index, std::size_t len ) {
		std::lock_guard<std::mutex> lock( ingestion_mutex );

		if( index_length.find( index ) != index_length.cend() ) {
			// printf( "index %d already present\n", id );
			return std::numeric_limits< std::size_t>::max();
		}
		std::size_t newLength = length += len;
		std::size_t oldLength = newLength - len;
		// printf( "inserting index %d\n", id );
		index_length.emplace( index,
			oldLength );
		return oldLength;
	}

	map_iter_t get_index_entry( int id ) const {

		typename std::unordered_map< int, std::size_t >::const_iterator el = index_length.find( id );
		if( el == ingestion->index_length.cend() ) {
			// printf( "index %d is absent\n", id );
			throw std::runtime_error( "index " + std::to_string( id ) + " not present" );
			// return static_cast< jlong >( std::numeric_limits< long >::max() );
		}
		return el;
	}

	matrix_entry_t * get_index_storage( int id ) {
		map_iter_t offset = get_index_entry( id );
		return entries + offset->second;
	}

	matrix_entry_t * get_begin() const {
		return entries;
	}

	matrix_entry_t * get_end() const {
		return entries + length.load();
	}
};

template< typename RowT, typename ColT > ingestion_data< RowT, ColT > *
	volatile ingestion_data< RowT, ColT >::ingestion = nullptr;
template< typename RowT, typename ColT >
	std::mutex ingestion_data< RowT, ColT >::ingestion_mutex;
