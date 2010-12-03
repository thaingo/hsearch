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
package com.bizosys.hsearch.dictionary;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import com.bizosys.oneline.util.StringUtils;

import com.bizosys.hsearch.common.IStorable;
import com.bizosys.hsearch.common.Storable;

public class DictEntry implements IStorable{
		
	public static final String TYPE_SEPARATOR = "\t";
	
	public String fldWord = null;
	public String fldType = null;
	public int fldFreq = 1;
	public String fldRelated = null;
	public String fldDetailXml = null;
	

	private DictEntry(){}
	
	public DictEntry(String fldWord) {
		this.fldWord = fldWord;
	}
	
	
	public DictEntry(String fldWord, String fldType, 
		int fldFreq, String related, String fldDetailXml) {
		
		this.fldWord = fldWord;
		this.fldType = fldType;
		this.fldFreq = fldFreq;
		this.fldRelated = related;
		this.fldDetailXml = fldDetailXml;
	}

	public DictEntry(String fldWord, String fldType, Integer fldFreq ) {
		this.fldWord = fldWord;
		this.fldType = fldType;
		this.fldFreq = fldFreq;
	}
	
	//Can only add once
	public void addRelatedWord(String related) {
		if  (HLog.l.isDebugEnabled()) 
			HLog.l.debug(" Related " + related);
		this.fldRelated = related;
	}
	
	/**
	 * Serialize the elements
	 */
	public byte[] toBytes() {
		
		byte[] fldWordB = ( null == fldWord) ? null : Storable.putString(fldWord);
		byte[] fldTypeB = ( null == fldType) ? null : Storable.putString(fldType);
		byte[] fldFreqB = Storable.putInt(fldFreq);
		byte[] fldRelatedB = ( null == fldRelated) ? null : Storable.putString(fldRelated);
		byte[] fldDetailXmlB = ( null == fldDetailXml) ? null : Storable.putString(fldDetailXml);
		
		int fldWordLen = ( null == fldWordB) ? 0 : fldWordB.length;
		int fldTypeLen = ( null == fldTypeB) ? 0 : fldTypeB.length;
		int fldRelatedLen = ( null == fldRelatedB) ? 0 : fldRelatedB.length;
		int fldDetailXmlLen = ( null == fldDetailXmlB) ? 0 : fldDetailXmlB.length;
		
		int totalBytes = fldWordLen + fldTypeLen + 
			fldFreqB.length + fldRelatedLen + fldDetailXmlLen;
		
		byte[] fldWordLenB = Storable.putShort((short) fldWordLen);
		byte[] fldTypeLenB = Storable.putShort((short) fldTypeLen);
		byte[] fldRelatedLenB = Storable.putShort((short) fldRelatedLen);
		byte[] fldDetailXmlLenB = Storable.putShort((short) fldDetailXmlLen);
		
		byte[] value = new byte[totalBytes + 8]; 
		int pos = 0;
		
		System.arraycopy(fldWordLenB, 0, value, pos, 2);
		pos = pos + 2;
		if ( 0 != fldWordLen) {
			System.arraycopy(fldWordB, 0, value, pos, fldWordLen);
			pos = pos + fldWordLen;
		}
		
		System.arraycopy(fldTypeLenB, 0, value, pos, 2);
		pos = pos + 2;
		if ( 0 != fldTypeLen) {
			System.arraycopy(fldTypeB, 0, value, pos, fldTypeLen);
			pos = pos + fldTypeLen;
		}
		
		System.arraycopy(fldFreqB, 0, value, pos, fldFreqB.length);
		pos = pos + fldFreqB.length;

		System.arraycopy(fldRelatedLenB, 0, value, pos, 2);
		pos = pos + 2;
		if ( 0 != fldRelatedLen) {
			System.arraycopy(fldRelatedB, 0, value, pos, fldRelatedLen);
			pos = pos + fldRelatedLen;
		}
		
		System.arraycopy(fldDetailXmlLenB, 0, value, pos, 2);
		pos = pos + 2;
		if ( 0 != fldDetailXmlLen) {
			System.arraycopy(fldDetailXmlB, 0, value, pos, fldDetailXmlLen);
			pos = pos + fldDetailXmlLen;
		}
		return value;
	}
	
	public DictEntry ( byte[] value) {
		int pos = 0;

		short fldWordLen = Storable.getShort(pos, value);
		pos = pos + 2;
		if ( 0 != fldWordLen) {
			byte[] fldWordB = new byte[fldWordLen];
			System.arraycopy(value, pos, fldWordB, 0, fldWordLen);			
			this.fldWord = Storable.getString(fldWordB);
			pos = pos + fldWordLen;
		}
		
		short fldTypeLen = Storable.getShort(pos, value);
		pos = pos + 2;
		if ( 0 != fldTypeLen) {
			byte[] fldTypeB = new byte[fldTypeLen];
			System.arraycopy(value, pos, fldTypeB, 0, fldTypeLen);
			this.fldType = Storable.getString(fldTypeB);
			pos = pos + fldTypeLen;
		}
		
		this.fldFreq = Storable.getInt(pos, value);
		pos = pos + 4;
		
		short fldRelatedLen = Storable.getShort(pos, value);
		pos = pos + 2;
		if ( 0 != fldRelatedLen) {
			byte[] fldRelatedB = new byte[fldRelatedLen];
			System.arraycopy(value, pos, fldRelatedB, 0, fldRelatedLen);			
			this.fldRelated = Storable.getString(fldRelatedB);
			pos = pos + fldRelatedLen;
		}

		short fldDetailXmlLen = Storable.getShort(pos, value);
		pos = pos + 2;
		if ( 0 != fldDetailXmlLen) {
			byte[] fldDetailXmlB = new byte[fldDetailXmlLen];
			System.arraycopy(value, pos, fldDetailXmlB, 0, fldDetailXmlLen);			
			this.fldDetailXml = Storable.getString(fldDetailXmlB);
			pos = pos + fldDetailXmlLen;
			
		}
	}
	
	public void addType(String type) {
		if ( null == type) return;
		if ( TYPE_SEPARATOR.equals(type) ) type = "    ";
		if ( null == this.fldType) {
			this.fldType = type;
			return;
		}
		
		//Merge
		if ( this.fldType.indexOf(type) < 0)
			this.fldType = this.fldType + TYPE_SEPARATOR + type;
	}
	
	/**
	 * Check all the types available
	 * @return
	 */
	public List<String> getTypes() {
	    if (StringUtils.isEmpty(this.fldType)) return null;
	    StringTokenizer tokenizer = new StringTokenizer (this.fldType, TYPE_SEPARATOR);
	    List<String> values = new ArrayList<String>();
	    while (tokenizer.hasMoreTokens()) {
	    	String token = tokenizer.nextToken();
	    	if (StringUtils.isEmpty(token)) continue;
	    	values.add(token);
	    }
	    return values;
	}
	
	public void toXml(Writer writer) throws IOException {
		writer.append("<e>");
		writer.append("<w>").append(this.fldWord).append("</w>");
		writer.append("<t>").append(this.fldType).append("</t>");
		writer.append("<f>").append(new Integer(this.fldFreq).toString()).append("</f>");
		writer.append("<r>").append(this.fldRelated).append("</r>");
		writer.append("<d>").append(this.fldDetailXml).append("</d>");
		writer.append("</e>");
	}
	
	
	@Override 
	public String toString() {
		StringBuilder sb = new StringBuilder(100);
		sb.append(" Word:").append(this.fldWord);
		sb.append(" , Type:").append(this.fldType);
		sb.append(" , Freq:").append(this.fldFreq);
		sb.append(" , Related:").append(this.fldRelated);
		sb.append(" , Detail:").append(this.fldDetailXml);
		return sb.toString();
	}
}