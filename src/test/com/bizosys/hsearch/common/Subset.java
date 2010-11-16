package com.bizosys.hsearch.common;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Test;
import junit.framework.TestCase;

import com.bizosys.ferrari.TestFerrari;

public class Subset extends TestCase {

	public static void main(String[] args) throws Exception {
        Test t = new ByteFieldTest();
        TestFerrari.testI18N(t);
	}
	
	public void search(String body, String text1, String text2, String  text3) {
		char[] firsts = new char[] {
			text1.charAt(0), text2.charAt(0), text3.charAt(0)
		};
		
		char[] lasts = new char[] {
				text1.charAt(text1.length() - 1),
				text2.charAt(text2.length() - 1), 
				text3.charAt(text3.length() - 1)
		};
		
		char[] chars = body.toLowerCase().toCharArray();
		int charsT = chars.length;
		for ( int i=0; i<charsT; i++) {
			char c = chars[i];
			if (i ==0 || c == ' ') {
				//Start of a word
				for (char d : firsts) {
					if ( c[i] == d ) 
				}
			}
		}
	}

}
