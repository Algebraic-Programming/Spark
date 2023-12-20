
#include <stdlib.h>
#include <stdio.h>

#include "com_huawei_graphblas_Native.h"

JNIEXPORT void JNICALL Java_com_huawei_graphblas_Native_setThreads(
		JNIEnv *,
		jclass,
		jint threads
    )
{
    char value[ 17 ];
    snprintf( value, 16, "%d", static_cast< int >( threads ) );
    setenv( "OMP_NUM_THREADS", value, 1 );
}
