#!/bin/bash

source config.conf

CDIR="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

CMD="${SPARK_HOME}/bin/spark-submit --conf spark.executor.extraLibraryPath=${GRB_INSTALL_PATH}/lib:${GRB_INSTALL_PATH}/lib/${GRB_BACKEND_LIB}:${CDIR}/build: --jars build/graphBLAS.jar --master spark://$(hostname):7077"

### UNCOMMENT THE EXAMPLE YOU WANT TO RUN

# ${CMD} --class com.huawei.graphblas.examples.Initialise examples/examples.jar 1

# ${CMD} --class com.huawei.graphblas.examples.SparkPagerank examples/examples.jar 1 $(pwd) $(pwd)/gyro_m/gyro_m.mtx

${CMD} --class com.huawei.graphblas.examples.Pagerank examples/examples.jar 1 $(pwd)/gyro_m/gyro_m.mtx

