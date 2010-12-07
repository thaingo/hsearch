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
package com.bizosys.hsearch.common;

import java.util.List;

import com.bizosys.hsearch.hbase.NVBytes;

/**
 * A Record 
 * @author bizosys
 *
 */
public class RecordBytes {
	public byte[] pk = null;
	public List<NVBytes> nvs = null;

	public RecordBytes(byte[] pk, List<NVBytes> nvs ) {
		this.pk = pk;
		this.nvs = nvs;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("  PK :").append(this.pk);
		if ( null == nvs) return sb.toString();
		for (NVBytes nv : nvs) {
			sb.append(nv.toString()).append('\n');
		}
		return sb.toString();
	}

}
