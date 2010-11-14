package com.bizosys.hsearch.dictionary;

import junit.framework.TestCase;


import com.bizosys.ferrari.TestFerrari;
import com.bizosys.hsearch.dictionary.DictEntry;

public class DictEntryTest extends TestCase {

	public static void main(String[] args) throws Exception {
		DictEntryTest t = new DictEntryTest();
        TestFerrari.testAll(t);
	}
	
	public void goodValues(String keyword, String type, Integer freq,
			String related, String detail) throws Exception {
		DictEntry e1 = new DictEntry(keyword,type,freq, related,detail);
		DictEntry e2 = new DictEntry(e1.toBytes()) ;
		assertEquals(detail, e2.fldDetailXml);
		assertEquals(freq.intValue(), e2.fldFreq);
		assertEquals(related, e2.fldRelated);
		assertEquals(type, e2.fldType);
		assertEquals(keyword, e2.fldWord);
	}

	public void nullValues(String keyword) throws Exception {
		DictEntry e1 = new DictEntry(keyword);
		DictEntry e2 = new DictEntry(e1.toBytes()) ;
		assertEquals(e1.fldDetailXml, e2.fldDetailXml);
		assertEquals(e1.fldFreq, e2.fldFreq);
		assertEquals(e1.fldRelated, e2.fldRelated);
		assertEquals(e1.fldType, e2.fldType);
		assertEquals(e1.fldWord, e2.fldWord);
	}
}
