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
package com.bizosys.hsearch.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;

public class TermFilter implements Filter {
	public byte[]  B;
	public byte[]  H;
	private byte[] matchedTLBytes = null;

	public TermFilter(){}
	public TermFilter( byte[] bytes){
		this.B=bytes;
		this.H = new byte[]{B[0],B[1],B[2],B[3]};
	}

	public boolean filterAllRemaining() {
		return false;
	}

	/**
	 *  true to drop this key/value
	 */
	public ReturnCode filterKeyValue(KeyValue kv) {
		matchedTLBytes = null;
		boolean isMatched = FilterIds.isMatchingBucket(kv.getRow(),B);
		if ( isMatched ) {
			matchedTLBytes = FilterIds.isMatchingColBytes(kv.getValue(), B);
			isMatched = ( null != matchedTLBytes);
			if (isMatched ) return ReturnCode.INCLUDE;
		}
		return ReturnCode.SKIP;
	}

	public boolean filterRow() {
		return false;
	}

	/**
	 * last chance to drop entire row based on the sequence of filterValue() 
	 * calls. Eg: filter a row if it doesn't contain a specified column
	 */
	public void filterRow(List<KeyValue> kvL) {
		if ( null == matchedTLBytes) return;
		if ( null == kvL) return;
		if ( 0 == kvL.size()) return;
		KeyValue kv = kvL.get(0);
		kvL.clear();
		kvL.add(new KeyValue(kv.getRow(),
			kv.getFamily(), kv.getQualifier(), matchedTLBytes));
	}
	
	/**
	 * true to drop this row, if false, we will also call
	 */
	public boolean filterRowKey(byte[] rowKey, int offset, int length) {
		return false;
	}
	
	public KeyValue getNextKeyHint(KeyValue arg0) {
		return null;
	}
	
	public boolean hasFilterRow() {
		return true;
	}
	
	public void reset() {
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int T = FilterIds.readHeader(in);
		this.B = new byte[T];
		in.readFully(this.B, 0, T);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		int BT = B.length;
		FilterIds.writeHeader(out, BT);
		out.write(B);
	}
}
