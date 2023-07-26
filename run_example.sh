#!/bin/bash

example_num=$1

if [[ -z "${example_num}" ]]; then
    example_num=1
fi

re='^[0-9]+$'
if [[ ! "${example_num}" =~ ${re} ]] ; then
   echo "error: argument is not a number"
   exit 1
fi



if ((example_num < 1 || example_num > 3)); then
    echo "select an example between 1 and 3"
    exit 1
fi

CDIR="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

source ${CDIR}/config.conf

# additional paths for Spark to locate for ALP and ALP/Spark binaries
ALP_BIN_PATHS="--conf spark.executor.extraLibraryPath=${GRB_INSTALL_PATH}/lib:${GRB_INSTALL_PATH}/lib/${GRB_BACKEND_LIB}:${CDIR}/build:"

# additional path for Spark to locate ALP/Spark JARs
ALP_BIN_JARS="--jars ${CDIR}/build/graphBLAS.jar"

# main command
CMD_BASE="${SPARK_HOME}/bin/spark-submit ${ALP_BIN_PATHS} ${ALP_BIN_JARS} --master spark://$(hostname):7077"

case "${example_num}" in
# Example 0: test only initialization for ALP/Spark
    1)
        ARGS="--class com.huawei.graphblas.examples.Initialise build/examples.jar 1"
        ;;
# Example 1: run PageRank in pure Spark implementation
    2)
        ARGS="--class com.huawei.graphblas.examples.SparkPagerank build/examples.jar 1 $(pwd) $(pwd)/gyro_m/gyro_m.mtx"
        ;;
# Example 2: run Pagerank in ALP/Spark implementation
    3)
        ARGS="--class com.huawei.graphblas.examples.Pagerank build/examples.jar 1 $(pwd)/gyro_m/gyro_m.mtx"
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
