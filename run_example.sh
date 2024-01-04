#!/bin/bash

function print_help {
    echo "usage run_example.sh [<options>...] <example number, 1-6> <example arguments, if any>..."
    echo "--master <URL to Spark master>"
    echo "--help this help"
}

CDIR="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

source ${CDIR}/config.conf

master_url=""
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

ARGS=$@

re='^[0-9]+$'
if [[ ! "${example_num}" =~ ${re} ]] ; then
   echo "error: argument is not a number"
   print_help
   exit 1
fi

if [[ -z "${master_url}" ]]; then
    master_url="spark://$(hostname):7077"
fi

VERSION="0.1.0"

# additional path for Spark to locate ALP/Spark JARs
ALP_BIN_JARS="--jars ${CDIR}/build/graphBLAS.jar"

# main command
CMD_BASE="${SPARK_HOME}/bin/spark-submit ${ALP_BIN_JARS} --master ${master_url}"

case "${example_num}" in
# Example 1: test only initialization for ALP/Spark
    1)
        echo "Info: this is a basic check of the ALP/Spark integration. Any given number of iterations is ignored."
        CLASS="Initialise"
        ;;
# Example 2: run PageRank in pure Spark implementation
    2)
        CLASS="SparkPagerank"
        echo "Info: this is a true PageRank using native Spark RDDs, which runs to convergence OR to the given max."
        ;;
# Example 3: run Pagerank in ALP/Spark implementation, reading te inpit file in ALP (single core, VERY SLOW!!!)
    3)
        CLASS="ALPPageRankFile"
        echo "Info: this is a true PageRank using ALP/Spark which runs to convergence. The input file is parsed in ALP single core, and can be VERY SLOW! This option of for testing purposes only."
        ;;
# Example 4: run Pagerank in ALP/Spark implementation
    4)
        CLASS="ALPPageRankRDD"
        echo "Info: this is a true PageRank using ALP/Spark which runs to convergence. The input file is parsed via Spark in parallel, as in typical user applications."
        ;;
# Example 5: run GraphX Pagerank, Pregel-variant (PageRank-like), un-normalised
    5)
        CLASS="GraphXPageRank"
        echo "Info: this is a PageRank-like (Pregel variant) of the Spark GraphX page ranking, which runs for 10 iterations."
        ;;
# Example 6: run GraphX Pagerank, normalised (still the Pregel variant)
    6)
        CLASS="NormalizedGraphXPageRank"
        echo "Info: this is a PageRank-like (Pregel variant, with normalization) of the Spark GraphX page ranking, which runs for 10 iterations."
        ;;
# Example 7: run GraphX Pagerank, normalised (still the Pregel variant)
    7)
        CLASS="SimpleSparkPagerank"
        echo "Info: this is a simple PageRank implementation, with periodic persistance to limit the lineage graph."
        ;;
    *)
        echo "unknown example number: ${example_num}"
        exit 1
        ;;
esac

CMD="${CMD_BASE} --class com.huawei.graphblas.examples.${CLASS} ${CDIR}/build/examples-assembly-${VERSION}.jar ${ARGS[@]}"

echo "Command: ===>"
echo ${CMD}
echo "<==="

${CMD}
