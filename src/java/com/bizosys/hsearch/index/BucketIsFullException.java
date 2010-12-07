/*
* Copyright 2010 The Apache Software Foundation
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.bizosys.hsearch.index;

/**
 * A backet has maximum capacity of 66534. More documents than 
 * this throws this exception. 
 * @author karan
 *
 */
public class BucketIsFullException extends Exception {

	private static final long serialVersionUID = 1L;
	
	/**
	 * Number of documents filled in the bucket.
	 */
	public long currentCount = 0;
	
	/**
	 * Constructor
	 * @param currentCount	Number of packed documents
	 */
	public BucketIsFullException(long currentCount) {
		super("Allowed 66534. Packed :" + currentCount);
		this.currentCount = currentCount;
	}
}