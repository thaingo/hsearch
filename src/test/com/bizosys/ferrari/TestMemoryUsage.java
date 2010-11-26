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

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Test;

public class TestMemoryUsage extends TestRandomValue{
	
	protected Map<String, Long> memUsages = new HashMap<String, Long>();
	protected int maxTolerableInKbs = 1024;
	public TestMemoryUsage(int maxTolerableInKbs) {
		this.maxTolerableInKbs = maxTolerableInKbs;
		super.setIterations(10);
	}
	
	private TestMemoryUsage() {
	}
	
	@Override
	public void run(Test testCase) throws Exception {
		super.verbose = false;
		super.run(testCase);
	}

	@Override
	protected void runMethod(Method runMethod, Test testCase) {
		System.out.println("\n... Checking Response Time test method >>" +  runMethod.getName() + "<<" );
		super.runMethod(runMethod, testCase);
	}
	
	@Override
	protected void invoke(Method runMethod, Test testCase, Object[] values) throws Exception {
		long startMem = Runtime.getRuntime().freeMemory();
		runMethod.invoke(testCase, values);
		long endMem = Runtime.getRuntime().freeMemory();
		long delta = ((startMem - endMem) / (1024)) ;
		String runMethodName = runMethod.getName();
		if ( memUsages.containsKey(runMethodName) ) {
			long avg =  (memUsages.get(runMethodName) + delta) / 2;
			memUsages.put(runMethodName, avg);
		} else {
			memUsages.put(runMethodName, delta);
		}
		
		if ( delta >= maxTolerableInKbs ) {
			failedFunctions.add(runMethod.getName());
		}
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(100);
		
		for (String key : memUsages.keySet()) {
			//int mem = memUsages.get(key).intValue();
			sb.append("  , ").append(key).append("=" ).append(memUsages.get(key)).append(" Kb");
		}
		
		return super.toString() + sb.toString();
	}
	
    public static void main(String[] args) throws Exception {
        Test testCase = new DryRunTest();
        TestMemoryUsage tester = new TestMemoryUsage(1024);
        tester.run(testCase);
        System.out.println(tester.toString());
    }
}