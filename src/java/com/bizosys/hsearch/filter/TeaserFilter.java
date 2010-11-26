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

public class TeaserFilter implements Filter {
	private static final byte TEASER_BYTE = "c".getBytes()[0];
	
	short cutLength = 360;
	byte[][] bWords = null;

	public TeaserFilter(){}
	
	public TeaserFilter(byte[][] bWords, short cutLength){
		this.bWords = bWords;
		this.cutLength = cutLength; 
	}
	
	public boolean filterAllRemaining() {
		return false;
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
	
		byte[] data = null;
		KeyValue kv = null;
		for ( int i=0; i< kvL.size(); i++ ) {
			kv = kvItr.next();
			if (TEASER_BYTE == kv.getQualifier()[0]) {
				BuildTeaserHighlighter bth = new BuildTeaserHighlighter();
				data = bth.find(kv.getValue(),bWords,cutLength);
				break;
			}
		}
		if ( null != data ) {
			kvItr.remove();
			kvL.add(new KeyValue(kv.getRow(), kv.getFamily(), kv.getQualifier(), data));
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
		this.cutLength = in.readShort();
		int len = in.readByte();
		int index = 1;
		this.bWords = new byte[len][];
		
		for ( int i=0; i<len; i++ ) {
			int wLen = in.readByte() ;
			index++;
			this.bWords[i] = new byte[wLen];
			in.readFully(this.bWords[i], 0, wLen);
			index = index + wLen;
		}
		
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeShort(cutLength);
		out.writeByte(bWords.length);
		for ( int i=0; i<bWords.length; i++ ) {
			out.writeByte(bWords[i].length);
			out.write(bWords[i]);
		}
	}

	public ReturnCode filterKeyValue(KeyValue arg0) {
		return ReturnCode.INCLUDE;
	}
}
