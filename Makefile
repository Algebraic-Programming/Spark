
#
#   Copyright 2021 Huawei Technologies Co., Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

include config.conf

.PHONY: all clean install

all: build/libsparkgrb.so build/graphBLAS.jar examples/examples.jar

GRBCXX=$(GRB_INSTALL_PATH)/bin/grbcxx

CXX=$(GRBCXX) -b $(GRB_BACKEND)

JNI_INCLUDE=$(patsubst %/bin/javac, %, $(realpath $(shell which $(JAVAC))))/include

CPPFLAGS=-O3 -DNDEBUG -g -I $(JNI_INCLUDE) -I $(JNI_INCLUDE)/linux

LFLAGS=

${GRB_INSTALL_PATH}/lib/spark/%: build/%
	mkdir -p ${GRB_INSTALL_PATH}/lib/spark || true
	cp "$<" "$@"

install: ${GRB_INSTALL_PATH}/lib/spark/libsparkgrb.so ${GRB_INSTALL_PATH}/lib/spark/graphBLAS.jar

build/graphBLAS.jar: build/com/huawei/Utils/package.class build/com/huawei/graphblas/Loader.class build/com/huawei/graphblas/Native.class build/com/huawei/GraphBLAS/package.class build/com/huawei/graphblas/PIDMapper.class
	cd build; jar cf "../$@" com

examples/examples.jar: examples/com/huawei/graphblas/examples/Initialise.class examples/com/huawei/graphblas/examples/SparkPagerank.class examples/com/huawei/graphblas/examples/Pagerank.class
	cd examples; jar cf "../$@" com

build/com_huawei_graphblas_Native.h: java/com/huawei/graphblas/Native.java java/com/huawei/graphblas/Loader.java
	mkdir build || true
	$(JAVAC) -cp java -d ./build -h ./build "$<"

build/com/huawei/graphblas/Loader.class: java/com/huawei/graphblas/Loader.java
	mkdir build || true
	$(JAVAC) -cp java -d ./build -h ./build "$<"

build/com/huawei/graphblas/Native.class: java/com/huawei/graphblas/Native.java
	mkdir build || true
	$(JAVAC) -cp java -d ./build "$<"

build/com/huawei/graphblas/PIDMapper.class: java/com/huawei/graphblas/PIDMapper.java
	mkdir build || true
	$(JAVAC) -cp java -d ./build "$<"

build/com/huawei/Utils/package.class: scala/com/huawei/Utils.scala
	mkdir build || true
	$(SCALAC) -cp "build:${SPARK_HOME}/conf/:${SPARK_HOME}/jars/*" -d ./build "$<"

build/com/huawei/GraphBLAS/package.class: scala/com/huawei/GraphBLAS.scala build/com/huawei/Utils/package.class build/com/huawei/graphblas/Loader.class build/com/huawei/graphblas/Native.class build/com/huawei/graphblas/PIDMapper.class
	mkdir build || true
	$(SCALAC) -cp "build:${SPARK_HOME}/conf/:${SPARK_HOME}/jars/*" -d ./build "$<"

build/native.o: cpp/native.cpp build/com_huawei_graphblas_Native.h cpp/sparkgrb.hpp
	${CXX} ${CPPFLAGS} -fPIC -Ibuild/ -I${GRB_INSTALL_PATH}/include/:./cpp/ -c -o "$@" "$<"

build/pagerank.o: cpp/pagerank.cpp cpp/sparkgrb.hpp
	${CXX} ${CPPFLAGS} -fPIC -Ibuild/ -I${GRB_INSTALL_PATH}/include/:./cpp/ -c -o "$@" "$<"

#This is an ugly workaround to an ALP bug (see GitHub issue 171)
CMD:="$(shell ${CXX} -b $(GRB_BACKEND) --show ${LFLAGS})"
PREFIX:=$(shell echo "${CMD}" | sed 's/\(^.*reference_omp\).*/\1/')
POSTFIX:=$(shell echo "${CMD}" | sed 's/^.*reference_omp\ \(.*\)/\1/')
MPOSTFIX:=$(shell echo "${POSTFIX}" | sed 's/\.a/\.so/g')

build/libsparkgrb.so: build/native.o build/pagerank.o
	${PREFIX} -shared -o "${@}" ${^} ${MPOSTFIX}
	# The following is bugged:
	#${CXX} -shared -o "${@}" ${^}

examples/com/huawei/graphblas/examples/%.class: scala/com/huawei/graphblas/examples/%.scala build/graphBLAS.jar
	mkdir examples || true
	$(SCALAC) -cp "build:${SPARK_HOME}/conf/:${SPARK_HOME}/jars/*" -d ./examples "$<"

clean:
	rm -r build
	rm -r examples

