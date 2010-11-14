package com.bizosys.ferrari;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import junit.framework.AssertionFailedError;
import junit.framework.Test;

import com.bizosys.hsearch.common.Storable;

public class TestZeroOrEmptyValue extends TestRandomValue{

	@Override
	protected void runMethod(Method runMethod, Test testCase){
		if (! Modifier.isPublic(runMethod.getModifiers())) {
			return;
		}
		System.out.println("... Running test method >>" +  runMethod.getName() + "<<" );
		Class[] params = runMethod.getParameterTypes();
		int iteration = 1;
		List<Object[]> values = new ArrayList<Object[]>(iteration);
		
		for (int i=0; i< iteration; i++) {
			values.add(new Object[params.length]); 
		}
		
		int counter = 0;
		for (Class param : params) {
			
			if ( String.class == param) {
				values.get(0)[counter] = "";
			} else if (Double.class == param) { 
				values.get(0)[counter] = 0;
			} else if (Long.class == param) { 
				values.get(0)[counter] = 0;
			} else if (Integer.class == param) { 
				values.get(0)[counter] = 0;
			} else if (Float.class == param) { 
				values.get(0)[counter] = 0.0;
			} else if (Short.class == param) { 
				values.get(0)[counter] = 0;
			} else if (Boolean.class == param) { 
				values.get(0)[counter] = false;
			} else if (Date.class == param) { 
				values.get(0)[counter] = new Date(0);
			} else if (Storable.class == param) {
				values.get(0)[counter] = new Storable("");
			}

			counter++;
		}
		try {
			totalRun++;
			for (Object[] objects : values) {
				invoke(runMethod, testCase, objects);
			}
			totalSucess++;
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			failedFunctions.add(runMethod.getName());
		}
	}
	
    public static void main(String[] args) throws Exception {
        Test testCase = new DryRunTest();
        TestZeroOrEmptyValue tester = new TestZeroOrEmptyValue(); 
        tester.run(testCase);
        System.out.println(tester.toString());
    }
	
}
