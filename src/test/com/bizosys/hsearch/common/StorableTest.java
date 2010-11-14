package com.bizosys.hsearch.common;

import java.nio.ByteBuffer;
import java.util.Date;

import junit.framework.Test;
import junit.framework.TestCase;


import com.bizosys.ferrari.TestFerrari;
import com.bizosys.hsearch.common.Storable;

public class StorableTest extends TestCase {

	public static void main(String[] args) throws Exception {
        Test t = new StorableListTest();
        TestFerrari.testAll(t);
	}
	
	public void testByteArray(String val) throws Exception {
		
		ByteBuffer bb = ByteBuffer.allocate(100);
		bb.put("Front Padding".getBytes());
		bb.putInt(3455);
		bb.put( "->".getBytes() );
		int startPos = bb.position();
		bb.put ( val.getBytes("UTF-8"));
		int endPos = bb.position();
		Storable storable = new Storable(bb.array(), Storable.BYTE_STRING, startPos, endPos);
		//System.out.println("Comparing : [" +  val + "] = [" + storable.asIsObject + "]");
		assertEquals(storable.asIsObject, val);
	}

	public void testString(String val) {
		
		Storable cold = new Storable(val);
		byte[] data =  cold.toBytes();
		
		Storable hot = new Storable(data, Storable.BYTE_STRING);
		//System.out.println("Comparing : [" +  val + "] = [" + hot.getValue() + "]");
		assertEquals(hot.getValue(), val);
	}
	
	public void testSerialization(String val) throws Exception {
		Storable hot = new Storable(val);
		assertTrue( 
			Storable.compareBytes(hot.toBytes(), val.getBytes("UTF-8") ) );
	}

	public void testShort(Short val) {
		ByteBuffer bb = ByteBuffer.allocate(2); 
		bb.putShort(val);
		Storable storable = new Storable(bb.array(), Storable.BYTE_SHORT);
		//System.out.println("Comparing : [" +  val + "] = [" + storable.asIsObject + "]");
		assertEquals(val, storable.asIsObject );
	}
	
	public void testSerializeShort(Short val) {
		Storable hot = new Storable(val);
		assertTrue( 
			Storable.compareBytes(hot.toBytes(), 
				ByteBuffer.allocate(2).putShort(val).array() ) );
	}
	
	
	public void testInt(Integer val) {
		ByteBuffer bb = ByteBuffer.allocate(4); 
		bb.putInt(val);
		Storable storable = new Storable(bb.array(), Storable.BYTE_INT);
		//System.out.println("Comparing : [" +  val + "] = [" + storable.asIsObject + "]");
		assertEquals(val, storable.asIsObject );
	}
	
	public void testSerializeInt(Integer val) {
		Storable hot = new Storable(val);
		assertTrue( 
			Storable.compareBytes(hot.toBytes(), 
				ByteBuffer.allocate(4).putInt(val).array() ) );
	}	
	
	public void testLong(Long val) {
		ByteBuffer bb = ByteBuffer.allocate(8); 
		bb.putLong(val);
		Storable storable = new Storable(bb.array(), Storable.BYTE_LONG);
		//System.out.println("Comparing : [" +  val + "] = [" + storable.asIsObject + "]");
		assertEquals(val, storable.asIsObject );
	}	

	public void testSerializeLong(Long val) {
		Storable hot = new Storable(val);
		assertTrue( 
			Storable.compareBytes(hot.toBytes(), 
				ByteBuffer.allocate(8).putLong(val).array() ) );
	}
	
	public void testFloat(Float val) {
		ByteBuffer bb = ByteBuffer.allocate(4); 
		bb.putFloat(val);
		Storable storable = new Storable(bb.array(), Storable.BYTE_FLOAT);
		//System.out.println("Comparing : [" +  val + "] = [" + storable.asIsObject + "]");
		assertEquals(val, storable.asIsObject );
	}	

	public void testSerializeFloat(Float val) {
		Storable hot = new Storable(val);
		assertTrue( 
			Storable.compareBytes(hot.toBytes(), 
				ByteBuffer.allocate(4).putFloat(val).array() ) );
	}
	
	public void testDouble(Double val) {
		ByteBuffer bb = ByteBuffer.allocate(8); 
		bb.putDouble(val);
		Storable storable = new Storable(bb.array(), Storable.BYTE_DOUBLE);
		//System.out.println("Comparing : [" +  val + "] = [" + storable.asIsObject + "]");
		assertEquals(val, storable.asIsObject );
	}
	
	public void testSerializeDouble(Double val) {
		Storable hot = new Storable(val);
		assertTrue( 
			Storable.compareBytes(hot.toBytes(), 
				ByteBuffer.allocate(8).putDouble(val).array() ) );
	}	
	
	public void testDate(Date val) {
		ByteBuffer bb = ByteBuffer.allocate(8); 
		bb.putLong(val.getTime());
		Storable storable = new Storable(bb.array(), Storable.BYTE_DATE);
		//System.out.println("Comparing : [" +  val + "] = [" + storable.asIsObject + "]");
		assertEquals(val, storable.asIsObject );
	}		
	
	public void testSerializeDate(Date val) {
		Storable hot = new Storable(val);
		assertTrue( 
			Storable.compareBytes(hot.toBytes(), 
				ByteBuffer.allocate(8).putLong(val.getTime()).array() ) );
	}	

	public void testStorable(byte[] bytes) {
		Storable storable = new Storable(bytes);
		assertEquals(bytes, storable.byteValue );
	}	
}
