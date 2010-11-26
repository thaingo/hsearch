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
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;

public class PreviewFilter implements Filter {
	private static final byte META_BYTE = "m".getBytes()[0];
	private static final byte ACL_BYTE = "a".getBytes()[0];
	FilterMetaAndAcl fma = null;
	byte[] bytes = null;

	public PreviewFilter(){}
	
	public PreviewFilter(FilterMetaAndAcl fma){
		this.fma = fma;
	}
	
	public FilterMetaAndAcl getFma(){
		return this.fma;
	}
	public void setFma(FilterMetaAndAcl fma) {
		this.fma = fma;
	}

	public boolean filterAllRemaining() {
		return false;
	}

	/**
	 *  true to drop this key/value
	 */
	public ReturnCode filterKeyValue(KeyValue kv) {
		if ( ACL_BYTE == kv.getQualifier()[0]) { // Match ACL
			if ( ! this.fma.allowAccess(kv.getValue())) {
				return ReturnCode.NEXT_ROW;
			}
		} else if (META_BYTE == kv.getQualifier()[0]) {
			if ( ! this.fma.allowMeta(kv.getValue())) {
				return ReturnCode.NEXT_ROW;
			}
		}
		return ReturnCode.INCLUDE;
	}

	public boolean filterRow() {
		return false;
	}

	/**
	 * last chance to drop entire row based on the sequence of filterValue() 
	 * calls. Eg: filter a row if it doesn't contain a specified column
	 */
	public void filterRow(List<KeyValue> kvL) {
		if ( null == kvL) return;
		if ( 0 == kvL.size()) return;
		Iterator<KeyValue> kvItr = kvL.iterator();
		for ( int i=0; i< kvL.size(); i++ ) {
			KeyValue kv = kvItr.next();
			if (ACL_BYTE == kv.getQualifier()[0]) kvItr.remove();
		}
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
		if ( null == this.fma ) this.fma = new FilterMetaAndAcl();
		this.fma.readHeader(in);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		if ( null == this.fma ) this.fma = new FilterMetaAndAcl();
		fma.writeHeader(out);
	}
}
