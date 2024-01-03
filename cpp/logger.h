
#pragma once

#include <stdio.h>

/*
levels:
FATAL
ERROR
WARNING
INFO
DEBUG
TRACE

*/

#define LOG_OFF 7

#define __FATAL_LEVEL 1
#define __ERROR_LEVEL 2
#define __WARNING_LEVEL 3
#define __INFO_LEVEL 4
#define __DEBUG_LEVEL 5
#define __TRACE_LEVEL 6

#ifndef LOG_LEVEL
	// #define LOG_LEVEL __WARNING_LEVEL
	#define LOG_LEVEL __INFO_LEVEL
#endif

#ifndef __LOG_LEVEL__
	#define __LOG_LEVEL__ ( LOG_LEVEL + 1 )
#endif


#if __LOG_LEVEL__ > __FATAL_LEVEL
	#define __LOGGER_FATAL( fmt, ... ) printf( fmt, ##__VA_ARGS__ )
#else
	#define __LOGGER_FATAL( fmt, ... )
#endif

#if __LOG_LEVEL__ > __ERROR_LEVEL
	#define __LOGGER_ERROR( fmt, ... ) printf( fmt, ##__VA_ARGS__ )
#else
	#define __LOGGER_ERROR( fmt, ... )
#endif

#if __LOG_LEVEL__ > __WARNING_LEVEL
	#define __LOGGER_WARNING( fmt, ... ) printf( fmt, ##__VA_ARGS__ )
#else
	#define __LOGGER_WARNING( fmt, ... )
#endif

#if __LOG_LEVEL__ > __INFO_LEVEL
	#define __LOGGER_INFO( fmt, ... ) printf( fmt, ##__VA_ARGS__ )
#else
	#define __LOGGER_INFO( fmt, ... )
#endif

#if __LOG_LEVEL__ > __DEBUG_LEVEL
	#define __LOGGER_DEBUG( fmt, ... ) printf( fmt, ##__VA_ARGS__ )
#else
	#define __LOGGER_DEBUG( fmt, ... )
#endif

#if __LOG_LEVEL__ > __TRACE_LEVEL
	#define __LOGGER_TRACE( fmt, ... ) printf( fmt, ##__VA_ARGS__ )
#else
	#define __LOGGER_TRACE( fmt, ... )
#endif


#define LOG( LEVEL, fmt, ... ) __LOGGER_ ## LEVEL( ">> " #LEVEL " >> " fmt "\n", ##__VA_ARGS__ )

#define LOG_INIT()

#define LOG_EXIT()
