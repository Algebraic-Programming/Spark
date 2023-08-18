

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

* LPF, for distributed execution; this, in turn, needs an MPI installation
* ALP
* JDK 11 or higher
* Scala SDK version 2.13
* Apache Spark 3.4

# Prerequisites

## LPF
You should install a modified LPF version with a few fixes (under integration in
the main branch) to better support the integration with Apache Spark; this
version can be found in the `spark_fixes` branch.
LPF, in turn, requires an MPI installation to be available.
Theoretically all MPI implementations should work properly; however, OpenMPI
does not currently work in practice due to a pending bug (at the time of
writing - see OpenMPI's
[GitHub issue](https://github.com/open-mpi/ompi/issues/6916)).
Therefore, we strongly advise to use MPICH-derived implementations, although
this may be different from the MPI implementation bundled with your network
adapter (e.g., OFED).
If an IBverbs library is available, the performance cost of a non-optimized MPI
implementation should be negligible when using the `ibverbs` engine.

You may compile it with the usual CMake workflow, optionally specifying several
LPF-related options

```bash
git clone https://github.com/Algebraic-Programming/LPF.git -b spark_fixes
cd LPF
mkdir build
export LPF_INSTALL_PATH=$(pwd)/install
MPI_HOME=<path to MPI implementation> ../bootstrap.sh --prefix=${LPF_INSTALL_PATH} --disable-doc -DLPF_HWLOC=/usr/lib/$(uname -i)-linux-gnu/ -DLIB_IBVERBS=/usr/lib/$(uname -i)-linux-gnu/libibverbs.so
make -j$(nprocs) install
```

where
- `LPF_HWLOC` points to the HWloc library (usually in `/usr/lib/$(uname -i)-linux-gnu/`)
- `LIB_IBVERBS` points to the IBverbs library (for InfiniBand devices only)

## ALP
You can install and build ALP from the [GitHub repository](https://github.com/Algebraic-Programming/ALP.git),
following the instructions in [its README](https://github.com/Algebraic-Programming/ALP#readme).
For a distributed setup, the `spark_fixes` branch is needed, as it stores
several modifications (to be integrated) for this scenario.
In summary, you should clone, configure, build and install it according to a
usual CMake workflow:

```bash
git clone https://github.com/Algebraic-Programming/ALP -b spark_fixes
cd ALP
mkdir build
cd build
export GRB_INSTALL_PATH=$(pwd)/install
../bootstrap.sh --prefix=${GRB_INSTALL_PATH} --with-lpf=${LPF_INSTALL_PATH}
make install -j$(nproc)
```

Note that you can change the argument of the `--prefix` option to install ALP in
any (empty) folder you may choose, and the `--with-lpf` option serves to point
ALP to the LPF installation.
For simplicity, from now on we will call the installation path as
`GRB_INSTALL_PATH`.

## JDK 11
You may tipically install JDK 11 from the packge management tool of your Linux
distribution; e.g., in Ubuntu 20.04 `apt-get install openjdk-11-jdk`. As an
alternative, you can download it from the
[Oracle website](https://www.oracle.com/java/technologies/javase/jdk11-archive-downloads.html)
and then add it conveniently to your `PATH` environment variable.

The build infrastructure assumes the `javac` command is available in the
environment.

## Scala 2.13
You can follow the [dedicated guide](https://www.scala-lang.org/download/2.13.0.html)
to install it and add it to your environment, in particular
[via the Coursier CLI](https://docs.scala-lang.org/getting-started/index.html#using-the-scala-installer-recommended-way).

The build infrastructure assumes the `scalac` command is available in the
environment.

## Apache Spark 3.4
You may follow the [official download instructions](https://spark.apache.org/downloads.html),
in particular selecting the package prebuilt for Scala 2.13 (2nd step of the
selection procedure). A direct link to the file is
https://dlcdn.apache.org/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3-scala2.13.tgz
(may change over time though).

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
git clone -b alp_spark_distributed https://github.com/Algebraic-Programming/Spark.git
cd Spark
ALP_SPARK_PATH=$(pwd)
```

From now on, we will assume the ALP/Spark prototype to be in the directory
`ALP_SPARK_PATH`.

To compile the ALP/Spark prototype, two steps are needed, namely *configuration*
and *compilation*. These are detailed in the following.

## Configuration
ALP/Spark needs to know the path of the Spark installation, that of the ALP
installation and finally the that of LPF installation. Both paths should be
manually written in the file `${ALP_SPARK_PATH}/config.conf`, as values for the
variables `SPARK_HOME`, `GRB_INSTALL_PATH` and `LPF_INSTALL_PATH`. Other
variables usually do not need changes.
Another important setting to consider is `LPF_ENGINE`, i.e., the LPF engine to
be used, by default set to `mpimsg`; this choice ensures best interoperability
because it leverages whatever communication channel the MPI installation can
work with, but may not guarantee best performance.
A better alternative for performance is the `ibverbs` engine, if available
within LPF.
You may refer to the comments inside `${ALP_SPARK_PATH}/config.conf` for more
details.

## Compilation
Simply issue `make` to produce `build/graphBLAS.jar` and `build/graphBLAS.so`.
This command also produces the LPF-Java wrapper, i.e. the command spawning Java
process as LPF processes (needed for distributed execution), stored in the
`${ALP_SPARK_PATH}/lpf_java_wrapper` directory.
Finally, it prints some options that should be manually inserted in the
configuration file for spark executors `${SPARK_HOME}/conf/spark-defaults.conf`.
Thes options add the JNI lookup paths to execute ALP native code from within
the Spark application.

# Running examples
You first need to start Apache Spark in standalone or cluster mode, for which
you may refer to the [official guide](https://spark.apache.org/docs/latest/).

As an example, Spark *standalone mode* allows manually spawning workers to
multiple machines by listing the hostnames in `${SPARK_HOME}/conf/workers.conf`.
Once this file is populated, the easiest way to
[start it standalone](https://spark.apache.org/docs/latest/spark-standalone.html#starting-a-cluster-manually),
is:

```bash
${SPARK_HOME}/sbin/start-master.sh
${SPARK_HOME}/sbin/start-workers.sh
```

Then you can submit ALP/Spark jobs to this master as normal Spark jobs, with
some additional options for Spark to locate the binary and JAR files generated
during compilation. The script `${ALP_SPARK_PATH}/run_examples.sh` shows
these variables and automates their generation; you may refer to it for more
details. This script also contains three examples to be selected via the command
line. For instance, the first example runs the PageRank algorithm on a matrix
ingested from an input file via ALP, resulting in much faster runtime compared
to the pure Spark implementation (second example). The example matrix used in
the script is [gyro_m from the SuitSparse Matrix Collection](https://sparse.tamu.edu/Oberwolfach/gyro_m),
read in `Matrix Market` format.
The quickest way to run the example is

```bash
cd ${ALP_SPARK_PATH}
wget https://suitesparse-collection-website.herokuapp.com/MM/Oberwolfach/gyro_m.tar.gz
tar -xf gyro_m.tar.gz
./run_example.sh 1
```

Then, you may see the Spark log on the standard output, with information about
the PageRank results.
similarly, you may select other examples by changing the script argument.
As a second argument (optional), you may pass the spark master URL; the default
is `spark://$(hostname):7077`.
