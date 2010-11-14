package com.bizosys.ferrari;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import junit.framework.Test;

public class TestI18N extends TestRandomValue {

	@Override
	protected void runMethod(Method runMethod, Test testCase) {
		if (! Modifier.isPublic(runMethod.getModifiers())) {
			return;
		}
		
		super.verbose = false;
		
		String methodName = runMethod.getName();
		if ( "main".equals(methodName) ) return;
		
		Class[] params = runMethod.getParameterTypes();
		int iteration = getIterations();
		List<Object[]> values = new ArrayList<Object[]>(iteration);
		
		for (int i=0; i< iteration; i++) {
			values.add(new Object[params.length]); 
		}
		
		int counter = 0;
		boolean isString = false;
		for (Class param : params) {
			
			if ( String.class == param) {
				isString = true;
				List<String> samples = DataRandomPrimitives.getUnicodeString(iteration);
				for ( int i=0; i< iteration; i++) values.get(i)[counter] = samples.get(i);
			} else if (Double.class == param) { 
				List<Double> samples = DataRandomPrimitives.getDouble(iteration);
				for ( int i=0; i< iteration; i++) values.get(i)[counter] = samples.get(i);
			} else if (Long.class == param) { 
				List<Long> samples = DataRandomPrimitives.getLong(iteration);
				for ( int i=0; i< iteration; i++) values.get(i)[counter] = samples.get(i);
			} else if (Integer.class == param) { 
				List<Integer> samples = DataRandomPrimitives.getInteger(iteration);
				for ( int i=0; i< iteration; i++) values.get(i)[counter] = samples.get(i);
			} else if (Float.class == param) { 
				List<Float> samples = DataRandomPrimitives.getFloat(iteration);
				for ( int i=0; i< iteration; i++) values.get(i)[counter] = samples.get(i);
			} else if (Short.class == param) { 
				List<Short> samples = DataRandomPrimitives.getShort(iteration);
				for ( int i=0; i< iteration; i++) values.get(i)[counter] = samples.get(i);
			} else if (Byte.class == param) { 
				List<Byte> samples = DataRandomPrimitives.getByte(iteration);
				for ( int i=0; i< iteration; i++) values.get(i)[counter] = samples.get(i);
			} else if (Boolean.class == param) { 
				List<Boolean> samples = DataRandomPrimitives.getBoolean(iteration);
				for ( int i=0; i< iteration; i++) values.get(i)[counter] = samples.get(i);
			} else if (Date.class == param) { 
				List<Date> samples = DataRandomPrimitives.getDates(iteration);
				for ( int i=0; i< iteration; i++) values.get(i)[counter] = samples.get(i);
			}

			counter++;
		}
		if ( ! isString ) return;  
		System.out.println("\n... Checking Internationalization | test method >>" +  methodName + "<<" );
		try {
			this.totalRun++;
			for (Object[] objects : values) {
				invoke(runMethod, testCase, objects);
			}
			this.totalSucess++;
		} catch (Exception ex) {
			failedFunctions.add(methodName);
			ex.printStackTrace(System.err);
		}
	}
	
    
    public static void main(String[] args) throws Exception {
        Test testCase = new DryRunTest();
    	new TestI18N().run(testCase);
    }
	
}
