package com.bizosys.hsearch.util;

import junit.framework.TestCase;

import com.bizosys.ferrari.TestFerrari;

public class UrlMapperTest extends TestCase {

	public static void main(String[] args) throws Exception {
		UrlMapperTest t = new UrlMapperTest();
        TestFerrari.testAll(t);
	}
	
	public void testSerialize() {
		UrlMapper mapper = UrlMapper.getInstance();
		String equalPrifix = "http://www.bizosys.com/employee.xml/id=23";
		String encoded = mapper.encoding(equalPrifix);
		System.out.println( "Encoding  [" + encoded + "]");
		System.out.println( "Decoding  [" + mapper.decoding(encoded) +"]" );

		System.out.println("\n\n\n");
		equalPrifix = "http://www.bizosys.com/employee?23";
		encoded = mapper.encoding(equalPrifix);
		System.out.println( "Encoding  [" + encoded + "]");
		System.out.println( "Decoding  [" + mapper.decoding(encoded) +"]" );
	
		System.out.println("\n\n\n");
		equalPrifix = "http://www.google.com/employee?23";
		encoded = mapper.encoding(equalPrifix);
		System.out.println( "Encoding  [" + encoded + "]");
		System.out.println( "Decoding  [" + mapper.decoding(encoded) +"]" );
	}
}
