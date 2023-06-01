#!/bin/bash

CDIR="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

source ${CDIR}/config.conf

# additional paths for Spark to locate for ALP and ALP/Spark binaries
ALP_BIN_PATHS="--conf spark.executor.extraLibraryPath=${GRB_INSTALL_PATH}/lib:${GRB_INSTALL_PATH}/lib/${GRB_BACKEND_LIB}:${CDIR}/build:"

# additional path for Spark to locate ALP/Spark JARs
ALP_BIN_JARS="--jars ${CDIR}/build/graphBLAS.jar"

# main command
CMD="${SPARK_HOME}/bin/spark-submit ${ALP_BIN_PATHS} ${ALP_BIN_JARS} --master spark://$(hostname):7077"

### UNCOMMENT THE EXAMPLE YOU WANT TO RUN

# test only initialization for ALP/Spark
# ${CMD} --class com.huawei.graphblas.examples.Initialise examples/examples.jar 1

# run PageRank in pure Spark implementation
# ${CMD} --class com.huawei.graphblas.examples.SparkPagerank examples/examples.jar 1 $(pwd) $(pwd)/gyro_m/gyro_m.mtx

# run Pagerank in ALP/Spark implementation
${CMD} --class com.huawei.graphblas.examples.Pagerank examples/examples.jar 1 $(pwd)/gyro_m/gyro_m.mtx

