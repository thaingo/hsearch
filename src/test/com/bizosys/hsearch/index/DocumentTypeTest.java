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

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import com.bizosys.ferrari.TestFerrari;
import com.bizosys.oneline.util.XmlUtils;
import com.thoughtworks.xstream.XStream;

public class DocumentTypeTest extends TestCase {

	public static void main(String[] args) throws Exception {
		DocumentTypeTest t = new DocumentTypeTest();
        //TestFerrari.testAll(t);
		t.testXML();
	}
	
	public void testSerialize() throws Exception {
		DocumentType type = DocumentType.getInstance();
		type.types.put("employee", (byte) -128);
		type.types.put("customer", (byte) -127);
		type.persist();
		assertTrue( (byte) -127 ==  type.types.get("customer"));
	}
	
	private void testXML() {
		Map<String, Byte> types = new HashMap<String, Byte>();
		types.put("employee", (byte) -128);
		types.put("customer", (byte) -127);
		System.out.println(XmlUtils.xstream.toXML(types));
	}
}
