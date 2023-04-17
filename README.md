

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


Introduction
============

This is an update of a very early version of GraphBLAS-accelerated Spark, first
presented by Suijlen and Yzelman in 2019 [1]. This version now compiles against
ALP v0.7. This code is a starting point for developing ALP/Spark, which is to be
a more thorough wrapper layer between ALP and Spark, with the aim of supporting
*all* algorithms that ALP currently provides. This README and code base will be
under significant flux as we work to achieve this goal, and in particular while
we test our approach for Spark 3.x and more recent JDKs and Scala distributions.
Therefore please consider this repository as a highly experimental code base, at
present.

[1] https://arxiv.org/abs/1906.03196initial


Compile-time dependencies
=========================

 * JDK 8 or higher
 * Spark 2.3.1 jars
 * Scala SDK version 2.11 (depends on the Spark version)
 * An MPI implementation, such as MPICH


Compilation & installation
==========================

Issue `make' to produce build/graphBLAS.jar and build/graphBLAS.so.

Put graphBLAS.jar in ${SPARK_HOME}/jars/ and make sure that directory is synced
across all workers when using Spark in cluster mode.

Put the graphBLAS.so file in a path of your choosing and configure Spark to add
your chosen path to its LD_LIBRARY_PATH. (TODO: check and add exactly how this
is done.)


Run-time dependencies
=====================

 * JRE 8 or higher
 * Spark 2.3.1 standalone or cluster

