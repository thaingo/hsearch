package com.bizosys.hsearch.lang;

import junit.framework.TestCase;

import com.bizosys.ferrari.TestFerrari;

public class StemmerTest extends TestCase {

	public static void main(String[] args) throws Exception {
		StemmerTest t = new StemmerTest();
        TestFerrari.testAll(t);
	}
	
	public void testSerialize() {
		Stemmer ls = Stemmer.getInstance();
		String[] words = new String[]{"onsequential", "corporating", "corporatist", "abatable",
				"abate","abatement", "abatic","abating", "abatis", "abattis",
				"abattoir","abaxial", "abaxially", "abaya" };
		
		for (String word : words) {
			System.out.println( word + "=" + ls.stem(word, true) + ":" + ls.stem(word));
		}
	}
}
