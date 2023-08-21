
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

.PHONY: all clean install jars cpp_clean java_wrapper emit_config

all: cpp_lib jars java_wrapper emit_config

GRBCXX=$(GRB_INSTALL_PATH)/bin/grbcxx

CXX=$(GRBCXX) -b $(GRB_BACKEND)

JNI_INCLUDE=$(patsubst %/bin/javac, %, $(realpath $(shell which $(JAVAC))))/include

# OPT_FLAGS=-O0
OPT_FLAGS=-O3 -DNDEBUG
CPPFLAGS=-g -I $(JNI_INCLUDE) -I $(JNI_INCLUDE)/linux $(OPT_FLAGS)

LFLAGS=

${GRB_INSTALL_PATH}/lib/spark/%: build/%
	mkdir -p ${GRB_INSTALL_PATH}/lib/spark || true
	cp "$<" "$@"

jars:
	sbt package


build/com_huawei_graphblas_Native.h: graphBLAS/src/main/java/com/huawei/graphblas/Native.java graphBLAS/src/main/java/com/huawei/graphblas/Loader.java
	mkdir build || true
	$(JAVAC) -cp java -d ./build -h ./build "$<"

build/native.o: cpp/native.cpp build/com_huawei_graphblas_Native.h cpp/sparkgrb.hpp cpp/pagerank.hpp
	${CXX} ${CPPFLAGS} -fPIC -Ibuild/ -I${GRB_INSTALL_PATH}/include/:./cpp/ -c -o "$@" "$<"

build/pagerank.o: cpp/pagerank.cpp cpp/sparkgrb.hpp cpp/pagerank.hpp
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

cpp_lib: build/libsparkgrb.so

cpp_clean:
	@rm -rf build/libsparkgrb.so build/*.o || true &> /dev/null

clean: cpp_clean
	sbt clean
	rm -rf build lpf_java_wrapper &> /dev/null

java_wrapper:
	@mkdir -p lpf_java_wrapper/bin
	@echo '#!/bin/bash' > lpf_java_wrapper/bin/java
	@echo >> lpf_java_wrapper/bin/java
	@echo '$(LPF_INSTALL_PATH)/bin/lpfrun -np 1 -engine $(LPF_ENGINE) $(shell which $(JAVA)) $$@' >> lpf_java_wrapper/bin/java

emit_config:
	@echo
	@echo "### you should place the following options inside <SPARK_HOME>/conf/spark-defaults.conf ###"
	@echo
	@echo "spark.executor.extraLibraryPath $(GRB_INSTALL_PATH)/lib:$(GRB_INSTALL_PATH)/lib/hybrid:$(shell pwd)/build # extra paths for libraries"
	@echo "spark.executorEnv.LPF_INIT NO # do not init LPF automatically"
	@echo "spark.executorEnv.JAVA_HOME $(shell pwd)/lpf_java_launcher # use the LPF-Java wrapper"
	@echo
#	@echo spark.scheduler.excludeOnFailure.unschedulableTaskSetTimeout 1000000000
#	@echo spark.executor.memory 400g

