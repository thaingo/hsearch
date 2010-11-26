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

import junit.framework.TestCase;


import com.bizosys.ferrari.TestFerrari;
import com.bizosys.hsearch.dictionary.DictEntry;

public class DictEntryTest extends TestCase {

	public static void main(String[] args) throws Exception {
		DictEntryTest t = new DictEntryTest();
        TestFerrari.testAll(t);
	}
	
	public void goodValues(String keyword, String type, Integer freq,
			String related, String detail) throws Exception {
		DictEntry e1 = new DictEntry(keyword,type,freq, related,detail);
		DictEntry e2 = new DictEntry(e1.toBytes()) ;
		assertEquals(detail, e2.fldDetailXml);
		assertEquals(freq.intValue(), e2.fldFreq);
		assertEquals(related, e2.fldRelated);
		assertEquals(type, e2.fldType);
		assertEquals(keyword, e2.fldWord);
	}

	public void nullValues(String keyword) throws Exception {
		DictEntry e1 = new DictEntry(keyword);
		DictEntry e2 = new DictEntry(e1.toBytes()) ;
		assertEquals(e1.fldDetailXml, e2.fldDetailXml);
		assertEquals(e1.fldFreq, e2.fldFreq);
		assertEquals(e1.fldRelated, e2.fldRelated);
		assertEquals(e1.fldType, e2.fldType);
		assertEquals(e1.fldWord, e2.fldWord);
	}
}
