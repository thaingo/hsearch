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
package com.bizosys.hsearch.util;

import junit.framework.TestCase;

import com.bizosys.ferrari.TestFerrari;

public class UrlShortnerTest extends TestCase {

	public static void main(String[] args) throws Exception {
		UrlShortnerTest t = new UrlShortnerTest();
        TestFerrari.testAll(t);
	}
	
	public void testSerialize() {
		UrlShortner mapper = UrlShortner.getInstance();
		String equalPrifix = "http://www.bizosys.com/employee.xml/id=23";
		String encoded = mapper.encoding(equalPrifix);
		System.out.println( "Encoding  [" + encoded + "]");
		System.out.println( "Decoding  [" + mapper.decoding(encoded) +"]" );

		System.out.println("\n\n\n");
		equalPrifix = "http://www.bizosys.com/employee?23";
		encoded = mapper.encoding(equalPrifix);
		System.out.println( "Encoding  [" + encoded + "]");
		System.out.println( "Decoding  [" + mapper.decoding(encoded) +"]" );
	
		System.out.println("\n\n\n");
		equalPrifix = "http://www.google.com/employee?23";
		encoded = mapper.encoding(equalPrifix);
		System.out.println( "Encoding  [" + encoded + "]");
		System.out.println( "Decoding  [" + mapper.decoding(encoded) +"]" );
	}
}
