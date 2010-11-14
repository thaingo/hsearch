package com.bizosys.ferrari;

import junit.framework.Test;
import junit.framework.TestCase;

public class DryRunTest extends TestCase {

	public static void main(String[] args) throws Exception {
        Test testCase = new DryRunTest();
        TestFerrari.testAll(testCase);
	}
	
	public void xSucess(String val) {
		assertTrue(val.equals(val));
	}

	public void testSucess() {
		xSucess("Abinash");
	}
	
	public void xFailure(String val) {
		assertTrue("~".equals(val));
	}

	public void testFailure() {
		xFailure("Abinash");
	}
	
	private void  dontRun() {
		assertTrue(1 ==1);
	}
	
}
