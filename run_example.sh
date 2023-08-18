#!/bin/bash

example_num=$1
master_url=$2

re='^[0-9]+$'
if [[ ! "${example_num}" =~ ${re} ]] ; then
   echo "error: argument is not a number"
   echo "usage: $0 <example number, 1-3> [<Spark master URL>]"
   exit 1
fi

if ((example_num < 1 || example_num > 3)); then
    echo "select an example between 1 and 3"
    exit 1
fi

CDIR="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

source ${CDIR}/config.conf

if [[ -z "${master_url}" ]]; then
	master_url="spark://$(hostname):7077"
fi

# additional paths for Spark to locate for ALP and ALP/Spark binaries

# additional path for Spark to locate ALP/Spark JARs
ALP_BIN_JARS="--jars ${CDIR}/build/graphBLAS.jar"

# main command
CMD_BASE="${SPARK_HOME}/bin/spark-submit ${ALP_BIN_JARS} --master ${master_url}"

case "${example_num}" in
# Example 0: test only initialization for ALP/Spark
    1)
        ARGS="--class com.huawei.graphblas.examples.Initialise build/examples.jar "
        ;;
# Example 1: run PageRank in pure Spark implementation
    2)
		mkdir $(pwd)/spark_persistence
        ARGS="--class com.huawei.graphblas.examples.SparkPagerank build/examples.jar $(pwd)/spark_persistence $(pwd)/gyro_m/gyro_m.mtx"
        ;;
# Example 2: run Pagerank in ALP/Spark implementation
    3)
        ARGS="--class com.huawei.graphblas.examples.Pagerank build/examples.jar $(pwd)/gyro_m/gyro_m.mtx"
        ;;
    *)
        echo "unknown example number: ${example_num}"
        exit 1
        ;;
esac

CMD="${CMD_BASE} ${ARGS}"

echo "Running: ===>"
echo ${CMD}
echo "<==="

${CMD}
