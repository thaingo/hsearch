package com.bizosys.hsearch.dictionary;

import junit.framework.TestCase;


import com.bizosys.ferrari.TestFerrari;
import com.bizosys.hsearch.dictionary.DistanceImpl;

public class DistanceTest extends TestCase {

	public static void main(String[] args) throws Exception {
		DistanceTest t = new DistanceTest();
        TestFerrari.testAll(t);
	}
	
	public void fitstCharDiff(String keyword) throws Exception {
		DistanceImpl dis = new DistanceImpl();
		int distance = dis.getDistance(keyword, keyword.substring(1));
		assertTrue( distance < 3);
	}
	
	public void lastCharDiff(String keyword) throws Exception {
		DistanceImpl dis = new DistanceImpl();
		int distance = dis.getDistance(keyword, keyword.substring(0, keyword.length() - 2 ));
		assertTrue( distance < 3);
	}
	
}
