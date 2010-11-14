package com.bizosys.hsearch.common;

import junit.framework.Test;
import junit.framework.TestCase;


import com.bizosys.ferrari.TestFerrari;
import com.bizosys.hsearch.common.ByteField;
import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.common.StorableList;

public class StorableListTest extends TestCase {

	public static void main(String[] args) throws Exception {
        Test t = new StorableListTest();
        TestFerrari.testAll(t);
	}
	
	public void testAdd1String(String val1) {
		//System.out.println("val1 = " + val1);
		StorableList sl = new StorableList();
		sl.add(new ByteField("f1", val1));

		StorableList s2 = new StorableList( sl.toBytes() );
		for (Object object : s2) {
			assertEquals(val1, Storable.getString( (byte[])object));
		}
	}
	
	public void testAdd2String(String val1, String val2) {
		//System.out.println("val1 = " + val1 + " , val2 = " + val2);
		StorableList sl = new StorableList();
		sl.add(new ByteField("f1", val1));
		sl.add(new ByteField("f2", val2));

		StorableList s2 = new StorableList( sl.toBytes() );
		assertEquals(val1, Storable.getString( (byte[])s2.get(0)));
		assertEquals(val2, Storable.getString( (byte[])s2.get(1)));
	}
	
	public void testAddInteger(Integer val1, Integer val2) {
		//System.out.println("val1 = " + val1 + " , val2 = " + val2);
		StorableList sl = new StorableList();
		sl.add(new ByteField("f1", val1));
		sl.add(new ByteField("f2", val2));

		StorableList s2 = new StorableList( sl.toBytes() );
		assertEquals(val1.intValue(), Storable.getInt(0, (byte[])s2.get(0)));
		assertEquals(val2.intValue(), Storable.getInt(0, (byte[])s2.get(1)));
	}	
	
}
