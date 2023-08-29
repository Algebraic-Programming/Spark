
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

/**
 * @file
 *
 * Implements a Spark thread to unique ID mapper.
 *
 * @author A. N. Yzelman
 * @date 20180903
 */


package com.huawei.graphblas;

import java.io.Serializable;

import java.lang.String;

import java.util.Arrays;
import java.util.HashMap;
import java.util.ArrayList;


/**
 * Helper class to map Spark executor threads to unique IDs.
 *
 * Also works with multiple Spark workers as long as each has a unique host
 * name.
 *
 * \warning This class thus will NOT work well if multiple Spark workers are
 *          deployed on the same node(!). Contact the maintainer if it
 *          would be useful to allow this.
 */
public class PIDMapper implements Serializable {

	private static final long serialVersionUID = 1L;

	/** Records the total number of Spark executors. */
	private int N;

	/** Records the total number of Spark workers. */
	private int P;

	/** Records the total number of local threads at each worker. */
	private ArrayList< Integer > T;

	/** Records Spark worker process IDs based on hostnames. */
	private HashMap< String, Integer > s;

	/** The master head node. */
	private String master;

	/**
	 * Constructs the mapper using a list of hostnames.
	 *
	 * @param[in] hostnames A list of hostnames.
	 *
	 * Each hostname should appear as many times as it has executor threads.
	 * A call to this function will sort the \a hostnames. If there are a
	 * great many hostnames this function may need revision.
	 */
	public PIDMapper( String hostnames[], String _master ) {
		N = hostnames.length;
		master = _master;
		if( N == 0 ) {
			P = 0;
		} else {
			Arrays.sort( hostnames, null );
			s = new HashMap< String, Integer >();
			s.put( hostnames[0], 0 );
			P = 1;
			T = new ArrayList< Integer >();
			T.add( 1 );
			for( int i = 1; i < N; ++i ) {
				if( !hostnames[i].equals( hostnames[i-1] ) ) {
					s.put( hostnames[i], P );
					++P;
					T.add( 1 );
				} else {
					T.set( P-1, T.get( P-1 ) + 1 );
				}
			}
		}
	}

	/**
	 * Used to retrieve the total number of threads at a given host.
	 *
	 * @param[in] hostname The host of which to retrieve the number of threads.
	 *
	 * @returns The number of threads at the given host.
	 */
	public int numThreads( String hostname ) {
		return T.get( processID( hostname ) );
	}

	/**
	 * Retrieves the process ID of the Spark worker at this host.
	 *
	 * @param[in] hostname The hostname of the current machine.
	 *
	 * @returns A unique process ID smaller than #numProcesses.
	 */
	public int processID( String hostname ) {
		return s.get(hostname);
	}

	/**
	 * Queries the number of hosts available.
	 *
	 * @returns The number of hosts available.
	 */
	public int numProcesses() {
		return P;
	}

	/**
	 * The host name that shall take on the role of starting SPMD sections.
	 *
	 * Other workers will connect to worker at the returned host name.
	 *
	 * @returns The head node for SPMD connection brokering.
	 */
	public String headnode() {
		return master;
	}

};

