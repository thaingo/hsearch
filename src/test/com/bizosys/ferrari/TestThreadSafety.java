package com.bizosys.ferrari;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Test;

public class TestThreadSafety extends TestRandomValue implements Runnable {

	public static void run(Test testCase, int threads, 
		List<String> results) throws Exception {
		
		for ( int i=0; i< threads; i++ ) {
			TestThreadSafety xx = new TestThreadSafety(results);
			xx.testCase = testCase;
			new Thread(xx).start();
		}
	}

	@Override
	protected void invoke(Method runMethod, Test testCase, Object[] values) throws Exception {
		long startTime = System.currentTimeMillis();
		runMethod.invoke(testCase, values);
		long endTime = System.currentTimeMillis();
		long delta = endTime - startTime;
		String runMethodName = runMethod.getName();
		if ( responseTimes.containsKey(runMethodName) ) {
			float avg =  (responseTimes.get(runMethodName) + delta) / 2;
			responseTimes.put(runMethodName, avg);
		} else {
			responseTimes.put(runMethodName, new Long(delta).floatValue());
		}
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(100);
		for (String key : responseTimes.keySet()) {
			sb.append("  , ").append(key).append("=" ).append(responseTimes.get(key)).append(" ms");
		}
		return super.toString() + sb.toString();
	}	
	
	public static String toString(int threads,List<String> results) throws Exception {
    	StringBuilder sb = new StringBuilder();
        for (String res : results) {
        	sb.append(res).append('\n');
		}
        return sb.toString();
	}

	public List<String> results = null;  
	public TestThreadSafety(List<String> results) {
		this.results = results;
	}
	
	
	public Test testCase;
	public void run() {
		try { 
			super.verbose = false;
			super.displayText = false;
			super.run(this.testCase);
			this.results.add(toString());
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}
	
    public static void main(String[] args) throws Exception {
        Test testCase = new DryRunTest();
        List<String> results = new ArrayList<String>();
        TestThreadSafety.run(testCase, 10, results);
        System.out.println(TestThreadSafety.toString(10, results));
    }
	
}
