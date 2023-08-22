#!/bin/bash

function print_help {
    echo "usage run_example.sh [<options>...] <example number, 1-5>"
    echo "--dataset <path to input dataset>"
    echo "--master_url <URL to Spark master>"
    echo "--persistence <path to directory for the persistence to Spark RDDs>"
    echo "--partitions <number of input partitions>"
}

CDIR="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

source ${CDIR}/config.conf

dataset=""
master_url=""
persistence=""
partitions=""
iterations="52"
run="no"
while true; do
  case "$1" in
    --help ) print_help; exit;;
    --dataset ) dataset="$2"; run="yes"; shift 2;;
    --master ) master_url="$2"; shift 2;;
    --persistence ) persistence="$2"; shift 2;;
    --partitions ) partitions="$2"; shift 2;;
    --* ) echo "unrecognized option: $1"; exit 1;;
    * ) break ;;
  esac
done

example_num=$1

re='^[0-9]+$'
if [[ ! "${example_num}" =~ ${re} ]] ; then
   echo "error: argument is not a number"
   print_help
   exit 1
fi

if ((example_num < 1 || example_num > 5)); then
    echo "select an example between 1 and 5"
    exit 1
fi

if [[ -z "${master_url}" ]]; then
    master_url="spark://$(hostname):7077"
fi


# additional path for Spark to locate ALP/Spark JARs
ALP_BIN_JARS="--jars ${CDIR}/build/graphBLAS.jar"

# main command
CMD_BASE="${SPARK_HOME}/bin/spark-submit ${ALP_BIN_JARS} --master ${master_url}"

case "${example_num}" in
# Example 1: test only initialization for ALP/Spark
    1)
        run="yes"
        ARGS="--class com.huawei.graphblas.examples.Initialise ${CDIR}/build/examples.jar"
        ;;
# Example 2: run PageRank in pure Spark implementation
    2)
        if [[ ! -z "${partitions}" && ! -z "${persistence}" && ! -z "${dataset}" ]]; then
            run="yes"
        else
            dataset="<path to input dataset>"
            master_url="<URL to Spark master>"
            persistence="<path to directory for the persistence to Spark RDDs>"
            partitions="<number of input partitions>"
        fi
        ARGS="--class com.huawei.graphblas.examples.SparkPagerank ${CDIR}/build/examples.jar ${partitions} ${persistence} ${iterations} ${dataset}"
        ;;
# Example 3: run Pagerank in ALP/Spark implementation
    3)
        if [[ ! -z "${dataset}" ]]; then
            run="yes"
        else
            dataset="<path to input dataset>"
            master_url="<URL to Spark master>"
            persistence="<path to directory for the persistence to Spark RDDs>"
            partitions="<number of input partitions>"
        fi
        ARGS="--class com.huawei.graphblas.examples.Pagerank ${CDIR}/build/examples.jar ${dataset}"
        ;;
# Example 4: run GraphX Pagerank, uncorrected
    4)
        if [[ ! -z "${persistence}" && ! -z "${dataset}" ]]; then
            run="yes"
        else
            dataset="<path to input dataset>"
            master_url="<URL to Spark master>"
            persistence="<path to directory for the persistence to Spark RDDs>"
            partitions="<number of input partitions>"
        fi
        ARGS="--class com.huawei.graphblas.examples.GraphXPageRank ${CDIR}/build/examples.jar ${persistence} ${iterations} false ${dataset}"
        ;;
# Example 5: run GraphX Pagerank, corrected (to be confirmed with GraphX source)
    5)
        if [[ ! -z "${persistence}" && ! -z "${dataset}" ]]; then
            run="yes"
        else
            dataset="<path to input dataset>"
            master_url="<URL to Spark master>"
            persistence="<path to directory for the persistence to Spark RDDs>"
            partitions="<number of input partitions>"
            iterations="<number of iterations>"
        fi
        ARGS="--class com.huawei.graphblas.examples.GraphXPageRank ${CDIR}/build/examples.jar ${persistence} ${iterations} true ${dataset}"
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

if [[ "${run}" == "yes" ]]; then
    ${CMD}
fi
