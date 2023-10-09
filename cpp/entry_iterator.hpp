
#pragma once

#include <iterator>

#include "matrix_entry.hpp"

template< typename RowIndexT, typename ColIndexT > class entry_iterator
{
public:

	using iterator_category = std::random_access_iterator_tag;
	using value_type = matrix_entry< RowIndexT, ColIndexT >;
	using difference_type = size_t;
	using reference = const value_type & ;
	using pointer = const value_type *;

	using RowIndexType = RowIndexT;
	using ColumnIndexType = ColIndexT;

	using this_t = entry_iterator< RowIndexT, ColIndexT >;

	// NonzeroStorage() = default;

	entry_iterator( pointer v ) noexcept :
		_val( v ) {}

	entry_iterator( this_t&& ) = default;

	entry_iterator( const this_t& ) = default;

	this_t& operator=( const this_t & ) = default;

	this_t& operator=( this_t && ) = default;

	// RowIndexT & i() { return this->_val->row; }
	const RowIndexT& i() const { return this->_val->row; }

	// ColIndexT & j() { return this->second; }
	const ColIndexT& j() const { return this->_val->col; }




	/** Equality check. */
	bool operator==( const this_t &other ) const {
		return _val == other._val;
	}

	/** Inequality check. */
	bool operator!=( const this_t &other ) const {
		return !operator==( other );
	};

	/** Increment operator. */
	this_t & operator++() {
		_val++;
		return *this;
	}

	/** Offset operator, enabled only for random access iterators */
	this_t & operator+=( std::size_t offset ) {
		_val += offset;
		return *this;
	}

	/** Difference operator, enabled only for random access iterators */
	difference_type operator-( const this_t & other ) const {
		return static_cast< difference_type >( this->_val - other._val );
	}

private:
	pointer _val;
};
