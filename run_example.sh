#!/bin/bash

function print_help {
    echo "usage run_example.sh [<options>...] <example number, 1-6> <example arguments, if any>..."
    echo "--master <URL to Spark master>"
    echo "--help this help"
}

CDIR="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

source ${CDIR}/config.conf

dataset=""
master_url=""
persistence=""
partitions=""
iterations="10"
run="no"
while true; do
  case "$1" in
    --help ) print_help; exit;;
    --master ) master_url="$2"; shift 2;;
    --* ) echo "unrecognized option: $1"; exit 1;;
    * ) break ;;
  esac
done

example_num=$1
shift 1

re='^[0-9]+$'
if [[ ! "${example_num}" =~ ${re} ]] ; then
   echo "error: argument is not a number"
   print_help
   exit 1
fi


if ((example_num < 1 || example_num > 6)); then
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
        echo "Info: this is a basic check of the ALP/Spark integration. Any given number of iterations is ignored."
        CLASS="Initialise"
        ;;
# Example 2: run PageRank in pure Spark implementation
    2)
        if [[ "$#" -ge "4" ]]; then
            run="yes"
            ARGS="$@"
        else
            ARGS="<number of input partitions> <path to directory for the persistence to Spark RDDs> <number of iterations> <path to input dataset(s)>"
        fi
        CLASS="SparkPagerank"
        echo "Info: this is a true PageRank using native Spark RDDs, which runs to convergence OR to the given max. number of iterations ${iterations}."
        ;;
# Example 3: run Pagerank in ALP/Spark implementation
    3)
        if [[ "$#" -ge "1" ]]; then
            run="yes"
            ARGS="$@"
        else
            ARGS="<path to input dataset(s)>"
        fi
        CLASS="PageRankFile"
        echo "Info: this is a true PageRank using ALP/Spark which runs to convergence. Any given number of iterations is ignored."
        ;;
    4)
        if [[ "$#" -ge "1" ]]; then
            run="yes"
            ARGS="$@"
        else
            ARGS="<path to input dataset(s)>"
        fi
        CLASS="PageRankRDD"
        echo "Info: this is a true PageRank using ALP/Spark which runs to convergence. Any given number of iterations is ignored."
        ;;
# Example 4: run GraphX Pagerank, Pregel-variant (PageRank-like), un-normalised
    5)
        if [[ "$#" -ge "3" ]]; then
            run="yes"
            ARGS="false $@"
        else
            ARGS="<path to directory for the persistence to Spark RDDs> <number of iterations> <path to input dataset(s)>"
        fi
        CLASS="GraphXPageRank"
        echo "Info: this is a PageRank-like (Pregel variant) of the Spark GraphX page ranking, which runs for 10 iterations."
        ;;
# Example 5: run GraphX Pagerank, normalised (still the Pregel variant)
    6)
        if [[ "$#" -ge "3" ]]; then
            run="yes"
            ARGS="true $@"
        else
            ARGS="<path to directory for the persistence to Spark RDDs> <number of iterations> <path to input dataset(s)>"
        fi
        CLASS="GraphXPageRank"
        echo "Info: this is a PageRank-like (Pregel variant, with normalization) of the Spark GraphX page ranking, which runs for 10 iterations."
        ;;
    *)
        echo "unknown example number: ${example_num}"
        exit 1
        ;;
esac

CMD="${CMD_BASE} --class com.huawei.graphblas.examples.${CLASS} ${CDIR}/build/examples.jar ${ARGS[@]}"

echo "Command: ===>"
echo ${CMD}
echo "<==="

if [[ "${run}" == "yes" ]]; then
    ${CMD}
fi
