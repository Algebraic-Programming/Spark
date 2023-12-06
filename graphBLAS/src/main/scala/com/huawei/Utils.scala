/*
 *   Copyright 2021 Huawei Technologies Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei

import java.net.InetAddress
import sun.misc.Unsafe
import java.lang.reflect.Field

import org.apache.spark.SparkEnv

package object Utils {


	final def getHostname() : String = {
		InetAddress.getLocalHost().toString; //.getCanonicalHostName()
	}

	final def getHostnameUnique() : String = {
		getHostname() + "-" + SparkEnv.get.executorId
	}

	final def computeMean[ T ]( seq: IndexedSeq[ T ] )(implicit num: Numeric[T] ): Double = {
		num.toDouble( seq.sum[ T ]( num ) ) / seq.length
	}

	final def computeStdDev[ T ]( seq: IndexedSeq[ T ] )(implicit num: Numeric[T] ): Double = {
		val mean = computeMean( seq )

		if( seq.length > 1 ) {
			seq.map( v  => Math.pow( num.toDouble( v ) - mean, 2 ) ).sum / seq.length.toDouble
		} else {
			Double.NaN
		}
	}

} // end object Utils

