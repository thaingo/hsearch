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
package com.bizosys.ferrari;

import junit.framework.TestCase;

import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.services.ServiceFactory;

public class Main extends TestCase {
	
	protected void setUp() {
		ServiceFactory.getInstance().init(new Configuration(), null);
	}
	
	protected void tearDown() {
		ServiceFactory.getInstance().stop();
	}

	public static void main(String[] args) throws Exception {
		Main test = new Main() ;
		test.setUp();
		
		TestRandomValue trv = new TestRandomValue(); 
		//trv.run(new FileFetcherTest());
		System.out.println(trv.toString());
		
		test.tearDown();
	}
	

}
