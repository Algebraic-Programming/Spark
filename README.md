

   Copyright 2021 Huawei Technologies Co., Ltd.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.


# Introduction
This is an update of a very early version of GraphBLAS-accelerated Spark, first
presented by Suijlen and Yzelman in 2019 [1]. This version now compiles against
ALP v0.7. This code is a starting point for developing ALP/Spark, which is to be
a more thorough wrapper layer between ALP and Spark, with the aim of supporting
*all* algorithms that ALP currently provides. This README and code base will be
under significant flux as we work to achieve this goal, and in particular while
we test our approach for Spark 3.x and more recent JDKs and Scala distributions.
Therefore please consider this repository as a highly experimental code base, at
present.

[1] https://arxiv.org/abs/1906.03196

At present, this code was tested to work with ALP shared-memory backend; testing
with the hybrid backend is ongoing.

# Compile-time dependencies

* ALP
* JDK 11 or higher
* Scala SDK version 2.13
* SBT 1.0.0 or higher
* Apache Spark 3.4

# Prerequisites

## ALP
You can install and build ALP from the [GitHub repository](https://github.com/Algebraic-Programming/ALP.git),
following the instructions in [its README](https://github.com/Algebraic-Programming/ALP#readme).
In summary, you should clone, configure, build and install it according to a
usual CMake workflow:

```bash
git clone https://github.com/Algebraic-Programming/ALP
cd ALP
mkdir build
cd build
mkdir install # installation directory
../bootstrap.sh --prefix=./install
make -j$(nproc)
make install -j$(nproc)
```

Note that you can change the argument of the `--prefix` option to install ALP in
any (empty) folder you may choose. For simplicity, from now on we will call this
path `GRB_INSTALL_PATH`.

## JDK 11
You may typically install JDK 11 from the packge management tool of your Linux
distribution; e.g., in Ubuntu 20.04 `apt-get install openjdk-11-jdk`. As an
alternative, you can download it from the
[Oracle website](https://www.oracle.com/java/technologies/javase/jdk11-archive-downloads.html)
and then add it to your `PATH` environment variable.

The build infrastructure assumes the `javac` command is available in the
environment.

## Scala 2.13
You can follow the [dedicated guide](https://www.scala-lang.org/download/2.13.0.html)
to install it and add it to your environment, in particular
[via the Coursier CLI](https://docs.scala-lang.org/getting-started/index.html#using-the-scala-installer-recommended-way).

The build infrastructure assumes the `scalac` command is available in the
environment.

## SBT
You may install SBT following [the official guide](https://www.scala-sbt.org/1.x/docs/Installing-sbt-on-Linux.html), or install it using the `Coursier` tool you may have
used for Scala, with `cs install sbt`.

## Apache Spark 3.4
You may follow the [official download instructions](https://spark.apache.org/downloads.html),
in particular selecting the package prebuilt for Scala 2.13 (2nd step of the
selection procedure). A direct link to the file is
https://dlcdn.apache.org/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3-scala2.13.tgz
(may change over time though), e.g.:

```bash
wget https://dlcdn.apache.org/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3-scala2.13.tgz
```

Once you have downloaded the package, you can extract it via the `tar` utility,
e.g.:

```bash
tar -xf spark-3.4.0-bin-hadoop3-scala2.13.tgz
```

This extracts a directory `spark-3.4.0-bin-hadoop3-scala2.13`, whose path we
will from now on indicate with `SPARK_HOME`. Inside this
path should be all the Spark components:

```bash
ls $SPARK_HOME
LICENSE  NOTICE  R  README.md  RELEASE  bin  conf  data  examples  jars  kubernetes  licenses  logs  python  sbin  work  yarn
```

# Compilation & installation
You should first clone this repository on this branch and enter the folder, if
you haven't done so yet:

```bash
git clone -b latest_scala https://github.com/Algebraic-Programming/Spark.git
cd Spark
ALP_SPARK_PATH=$(pwd)
```

From now on, we will assume the ALP/Spark prototype to be in the directory
`ALP_SPARK_PATH`.

To compile the ALP/Spark prototype, two steps are needed, namely *configuration*
and *compilation*. These are detailed in the following.

## Configuration
ALP/Spark needs to know the path of the Spark installation and of the ALP
installation.
Both paths should be manually written in the file
`${ALP_SPARK_PATH}/config.conf`, as values for the variables `SPARK_HOME` and
`GRB_INSTALL_PATH`, respectively.
Other variables usually do not need changes.
You may refer to the comments inside `${ALP_SPARK_PATH}/config.conf` for more
details.

## Compilation
Simply issue `make` from `ALP_SPARK_PATH` to produce both
`build/graphBLAS.jar` and `build/libsparkgrb.so`.

# Running examples
You first need to start Apache Spark in standalone or cluster mode, for which
you may refer to the [official guide](https://spark.apache.org/docs/latest/).
This example currently supports only the ALP shared-memory backend, which means
that one worker only is supported.
In particular, the easiest way is to
[start it standalone](https://spark.apache.org/docs/latest/spark-standalone.html#starting-a-cluster-manually),
for example:

```bash
${SPARK_HOME}/sbin/start-master.sh
${SPARK_HOME}/sbin/start-worker.sh spark://$(hostname):7077
```

Then you can submit ALP/Spark jobs to this master as normal Spark jobs, with
some additional options for Spark to locate the binary and JAR files generated
during compilation.

The script `${ALP_SPARK_PATH}/run_examples.sh` shows these variables and
automates their generation; you may refer to it for more details.
This script also contains three examples of Spark jobs:

1. an initialization job that only ensures the infrastructure works correctly
2. an implementation of PageRank in pure Spark, analyzing an input graph from command line
3. an implementation of PageRank in ALP, analyzing an input graph from command line

Examples 2 and 3 run the PageRank algorithm on a matrix ingested from an input
file; the example uses the
[gyro_m from the SuitSparse Matrix Collection](https://sparse.tamu.edu/Oberwolfach/gyro_m),
in `Matrix Market` format.
The quickest way to get it is

```bash
cd ${ALP_SPARK_PATH}
wget https://suitesparse-collection-website.herokuapp.com/MM/Oberwolfach/gyro_m.tar.gz
tar -xf gyro_m.tar.gz
```

Once the matrix is downloaded and extracted, you may select the example to run
by passing the number to the `${ALP_SPARK_PATH}/run_examples.sh` script, for
example

```bash
cd ${ALP_SPARK_PATH}
./run_examples.sh 2
```

If no number is passed, example 1 is run by default.
If you run a PageRank implementation, you may see the Spark log on the standard
output, with information about the PageRank results.
