
#pragma once

#pragma pack(1)
template< typename RowT, typename ColT >
struct matrix_entry {
	RowT row;
	ColT col;
};
