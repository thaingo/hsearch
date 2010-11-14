package com.bizosys.ferrari;

import junit.framework.TestCase;

import org.apache.oneline.conf.Configuration;
import org.apache.oneline.services.ServiceFactory;

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
