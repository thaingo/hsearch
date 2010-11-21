package com.bizosys.hsearch.index;

import junit.framework.TestCase;

import com.bizosys.ferrari.TestFerrari;

public class DocumentTypeTest extends TestCase {

	public static void main(String[] args) throws Exception {
		DocumentTypeTest t = new DocumentTypeTest();
        TestFerrari.testAll(t);
	}
	
	public void testSerialize() throws Exception {
		DocumentType type = new DocumentType();
		type.types.put("employee", (byte) -128);
		type.types.put("customer", (byte) -127);
		type.persist();
		assertTrue( (byte) -127 ==  type.types.get("customer"));
	}
}
