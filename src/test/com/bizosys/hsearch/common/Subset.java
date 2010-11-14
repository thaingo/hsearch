package com.bizosys.hsearch.common;

import java.util.Hashtable;
import java.util.Map;

public class Subset {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		long start = System.currentTimeMillis();
		StringBuilder sb = new StringBuilder(100);
		Map<String, Short> ranks = new Hashtable<String, Short>(300000);
		for ( int i=0; i<2500; i++ ) {
			for ( int j=0; j<10; j++) {
				sb.append(i).append('_').append(j);
				ranks.put(sb.toString(), (short) 12);
				sb.delete(0, 100);
			}
		}
		long end = System.currentTimeMillis();
		System.out.println(end - start);
	}

}
