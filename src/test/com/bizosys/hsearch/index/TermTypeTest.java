package com.bizosys.hsearch.index;

import junit.framework.TestCase;

import com.bizosys.ferrari.TestFerrari;

public class TermTypeTest extends TestCase {

	public static void main(String[] args) throws Exception {
		TermTypeTest t = new TermTypeTest();
        TestFerrari.testAll(t);
	}
	
	public void testSerialize() throws Exception {
		TermType type = new TermType();
		type.types.put("Employee", (byte) -128);
		type.types.put("customers", (byte) -127);
		type.persist();
		assertTrue( (byte) -127 ==  type.types.get("customer"));
	}
}
