package com.bizosys.hsearch.index;

import junit.framework.TestCase;

import com.bizosys.ferrari.TestFerrari;

public class TermTest extends TestCase {

	public static void main(String[] args) throws Exception {
		TermTest t = new TermTest();
        TestFerrari.testAll(t);
	}
	
	public void testSerialize() throws Exception {
		Term t = new Term();
		assertEquals(0 , t.getTermPos(t.setTermPos(0)) );
		assertEquals(23 , t.getTermPos(t.setTermPos(23)) );
		assertEquals(64998, t.getTermPos(t.setTermPos(64998)) );
		assertEquals(64999, t.getTermPos(t.setTermPos(64999)) );
		assertEquals(65000, t.getTermPos(t.setTermPos(65000)) );
		assertEquals(65001, t.getTermPos(t.setTermPos(65001)) );
		assertEquals(65002, t.getTermPos(t.setTermPos(65002)) );
		assertEquals(42234, t.getTermPos(t.setTermPos(42234)) );
		assertEquals(-1, t.getTermPos(t.setTermPos(-1)) );
		assertEquals(36435345, t.getTermPos(t.setTermPos(36435345)) );
		assertEquals(482324, t.getTermPos(t.setTermPos(482324)) );
		assertEquals(7823435, t.getTermPos(t.setTermPos(7823435)) );
		assertEquals(-2134324 ,t.getTermPos(t.setTermPos(-2134324)) );
		
		Term term = new Term();
		term.setTermFrequency((byte)122) ;
		
		Term term2 = new Term();
		term2.setTermFrequency((byte) 2);
		term.merge(term2);

		System.out.println("Term Frequency :" + term.getTermFrequency());
	}
}
