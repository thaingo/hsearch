/*
* Copyright 2010 The Apache Software Foundation
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.bizosys.hsearch.common;

import java.nio.ByteBuffer;

import junit.framework.Test;
import junit.framework.TestCase;


import com.bizosys.ferrari.TestFerrari;
import com.bizosys.hsearch.common.ByteField;

public class ByteFieldTest extends TestCase {

	public static void main(String[] args) throws Exception {
        Test t = new ByteFieldTest();
        TestFerrari.testAll(t);
	}
	
	public void testAlphaEqual(String alpha) {
		xBytesEqual(alpha.getBytes(), alpha.getBytes());
	}

	public void testAlphaNumericEqual() {
		xBytesEqual("Abin34ash12".getBytes(), "Abin34ash12".getBytes());
	}
	
	public void testAlphaCapitalEqual(String alpha) {
		alpha = alpha.toUpperCase();
		xBytesEqual(alpha.getBytes(), alpha.getBytes());
	}

	public void testNumberEqual() {
		xBytesEqual("234234".getBytes(), "234234".getBytes());
	}

	private void xBytesEqual(byte[] input1, byte[] input2) {
		assertEquals(true, ByteField.compareBytes(false, input1, input2));
	}

	public void testCapitalDiffer() {
		//	E capital
		xBytesInEqual("Abinash Kara is a fisher man with deoxyribonuceleciacid".getBytes(),
				"Abinash Kara is a fisher man with deoxyribonucelEciacid".getBytes());
	}	

	public void testNumberDiffer() {
		xBytesInEqual("Royal County 466".getBytes(),
			"Royal County 467".getBytes());
	}	

	public void testSpecialDiffer() {
		xBytesInEqual("!Harichandanpur".getBytes(),
			"Harichandanpur".getBytes());
	}	

	public void testBlankDiffer() {
		xBytesInEqual("KeonjharGarh".getBytes(), "".getBytes());
	}	
	
	public void testLengthDiffer() {
		xBytesInEqual("infosys".getBytes(), "wipro".getBytes());
	}	

	public void testBetweenDiffer() {
		xBytesInEqual("infosys".getBytes(), "injosys".getBytes());
	}	

	public void testLongEndDiffer() {
		xBytesInEqual("com.infosys.hr.abinash".getBytes(), 
			"com.infosys.hr.sunil".getBytes());
	}	

	private void xBytesInEqual(byte[] input1, byte[] input2) {
		assertEquals(false, ByteField.compareBytes(false, input1, input2));
	}
	

	public void testMatchingInteger(Integer a) {
		xNativeIntegerCompare(false, a, a);
		xOffsetIntegerCompare(false, a, a);
		xByteBufferIntegerCompare(false, a, a);
		
		xNativeIntegerCompare(true, a, a);
		xOffsetIntegerCompare(true, a, a);
		xByteBufferIntegerCompare(true, a, a);
	}	
	
	public void testAnyWithIntegerMin(Integer a) {
		xNativeIntegerCompare(true, Integer.MIN_VALUE, a);
		xOffsetIntegerCompare(true, Integer.MIN_VALUE, a);
		xByteBufferIntegerCompare(true, Integer.MIN_VALUE, a);
		xNativeIntegerCompare(false, Integer.MIN_VALUE, a);
		xOffsetIntegerCompare(false, Integer.MIN_VALUE, a);
		xByteBufferIntegerCompare(false, Integer.MIN_VALUE,a);
	}	
	
	public void testAnyWithIntegerMax(Integer a) {

		xNativeIntegerCompare(true, Integer.MAX_VALUE, a);
		xOffsetIntegerCompare(true, Integer.MAX_VALUE, a);
		xByteBufferIntegerCompare(true, Integer.MAX_VALUE, a);
		xNativeIntegerCompare(false, Integer.MAX_VALUE, a);
		xOffsetIntegerCompare(false, Integer.MAX_VALUE, a);
		xByteBufferIntegerCompare(false, Integer.MAX_VALUE, a);
	}	

	
	public void testAnyWithInteger0(Integer a) {
		
		xNativeIntegerCompare(true, 0, a);
		xOffsetIntegerCompare(true, 0, a);
		xByteBufferIntegerCompare(true, 0, a);
		xNativeIntegerCompare(false, 0, a);
		xOffsetIntegerCompare(false, 0, a);
		xByteBufferIntegerCompare(false, 0, a);
		
	}
	
	private void xNativeIntegerCompare(boolean isTypeBit, int value1, int value2) {
		
		ByteField a = new ByteField("Name1", value1);
		a.enableTypeOnToBytes(isTypeBit);
		byte[] identifierBits = a.toBytes();

		ByteField c = new ByteField("Name2", value2);
		c.enableTypeOnToBytes(false);
		byte[] origBits = c.toBytes();
		
		boolean expected = (value1 == value2);

		assertEquals(expected,
			ByteField.compareBytes(isTypeBit, identifierBits,origBits));
		
	}

	private void xOffsetIntegerCompare(boolean isTypeBit, int value1, int value2) {

		ByteField a = new ByteField("Name1", value1);
		a.enableTypeOnToBytes(isTypeBit);
		byte[] identifierBits = a.toBytes();

		byte[] shifts = new byte[identifierBits.length + 1];
		for ( int jh=0; jh< identifierBits.length ; jh++) shifts[jh+1] = identifierBits[jh];
		
		ByteField b = new ByteField("Name2", value2);
		b.enableTypeOnToBytes(false);
		byte[] origBits = b.toBytes();

		boolean expected = (value1 == value2);

		assertEquals(expected,
				 ByteField.compareBytes(isTypeBit,1,shifts, origBits) );
	}
	

	private void xByteBufferIntegerCompare(boolean isTypeBit, int value1, int value2) {

		ByteField a1 = new ByteField("A", value1);
		a1.enableTypeOnToBytes(isTypeBit);
		byte[] origBits = a1.toBytes();
		
		ByteBuffer bb = ByteBuffer.allocate(4);
		bb.putInt(value2);

		boolean expected = (value1 == value2);

		assertEquals(expected,
		ByteField.compareBytes(isTypeBit, origBits, bb.array()) );
	}
	
	
	/**
	 * Long test starts here
	 *
	 */
	public void testMatchingLongMin(Long a) {
		xNativeLongCompare(true, a, a);
		xOffsetLongCompare(true, a, a);
		xByteBufferLongCompare(true, a, a);
		xNativeLongCompare(false, a, a);
		xOffsetLongCompare(false, a, a);
		xByteBufferLongCompare(false, a, a);
	}	
	
	public void testNonMatchingLongMin(Long a) {
		xNativeLongCompare(true, Long.MIN_VALUE, a);
		xOffsetLongCompare(true, Long.MIN_VALUE, a);
		xByteBufferLongCompare(true, Long.MIN_VALUE, a);
		xNativeLongCompare(false, Long.MIN_VALUE, a);
		xOffsetLongCompare(false, Long.MIN_VALUE, a);
		xByteBufferLongCompare(false, Long.MIN_VALUE, a);
	}	
	
	public void testNonMatchingLongMax(Long a) {

		xNativeLongCompare(true, Long.MAX_VALUE, a);
		xOffsetLongCompare(true, Long.MAX_VALUE, a);
		xByteBufferLongCompare(true, Long.MAX_VALUE, a);
		xNativeLongCompare(false, Long.MAX_VALUE, a);
		xOffsetLongCompare(false, Long.MAX_VALUE, a);
		xByteBufferLongCompare(false, Long.MAX_VALUE, a);
	}	

	
	public void testNonMatchingLongZaro(Long a) {
		
		xNativeLongCompare(true, 0, a);
		xOffsetLongCompare(true, 0, a);
		xByteBufferLongCompare(true, 0, a);
		xNativeLongCompare(false, 0, a);
		xOffsetLongCompare(false, 0, a);
		xByteBufferLongCompare(false, 0, a);
	}		
	
	private void xNativeLongCompare(boolean isTypeBit, long value1, long value2) {
		
		ByteField a = new ByteField("Name1", value1);
		a.enableTypeOnToBytes(isTypeBit);
		byte[] identifierBits = a.toBytes();

		ByteField c = new ByteField("Name2", value2);
		c.enableTypeOnToBytes(false);
		byte[] origBits = c.toBytes();
		
		boolean expected = (value1 == value2);

		assertEquals(expected,
			ByteField.compareBytes(isTypeBit, identifierBits,origBits));
		
	}

	private void xOffsetLongCompare(boolean isTypeBit, long value1, long value2) {

		ByteField a = new ByteField("Name1", value1);
		a.enableTypeOnToBytes(isTypeBit);
		byte[] identifierBits = a.toBytes();

		byte[] shifts = new byte[identifierBits.length + 1];
		for ( int jh=0; jh< identifierBits.length ; jh++) shifts[jh+1] = identifierBits[jh];

		ByteField b = new ByteField("Name2", value2);
		b.enableTypeOnToBytes(false);
		byte[] origBits = b.toBytes();

		boolean expected = (value1 == value2);

		assertEquals(expected,
				 ByteField.compareBytes(isTypeBit,1,shifts, origBits) );
	}
	

	private void xByteBufferLongCompare(boolean isTypeBit, long value1, long value2) {

		ByteField c = new ByteField("A", value1);
		c.enableTypeOnToBytes(isTypeBit);
		byte[] origBits = c.toBytes();
		
		ByteBuffer bb = ByteBuffer.allocate(8);
		bb.putLong(value2);

		boolean expected = (value1 == value2);

		assertEquals(expected,
		ByteField.compareBytes(isTypeBit, origBits, bb.array()) );
	}
	

	/**
	 * Short
	 */
	/**
	 * Short test starts here
	 *
	 */
	public void testMatchingShort(Short a) {
		xNativeShortCompare(true, a, a);
		xOffsetShortCompare(true, a, a);
		xByteBufferShortCompare(true, a, a);
		xNativeShortCompare(false, a, a);
		xOffsetShortCompare(false, a, a);
		xByteBufferShortCompare(false, a, a);
	}	
	
	public void testNonMatchingShortMin(Short s) {
		xNativeShortCompare(true, Short.MIN_VALUE, s);
		xOffsetShortCompare(true, Short.MIN_VALUE, s);
		xByteBufferShortCompare(true, Short.MIN_VALUE, s);
		xNativeShortCompare(false, Short.MIN_VALUE, s);
		xOffsetShortCompare(false, Short.MIN_VALUE, s);
		xByteBufferShortCompare(false, Short.MIN_VALUE, s);
	}	
	
	public void testNonMatchingShortMax(Short s) {

		xNativeShortCompare(true, Short.MAX_VALUE, s);
		xOffsetShortCompare(true, Short.MAX_VALUE, s);
		xByteBufferShortCompare(true, Short.MAX_VALUE, s);
		xNativeShortCompare(false, Short.MAX_VALUE, s);
		xOffsetShortCompare(false, Short.MAX_VALUE, s);
		xByteBufferShortCompare(false, Short.MAX_VALUE, s);
	}	

	
	public void testNonMatchingShortZaro(Short s) {
		short zero = 0;
		xNativeShortCompare(true, zero, s);
		xOffsetShortCompare(true, zero, s);
		xByteBufferShortCompare(true, zero, s);
		xNativeShortCompare(false, zero, s);
		xOffsetShortCompare(false, zero, s);
		xByteBufferShortCompare(false, zero, s);
	}		
	
	private void xNativeShortCompare(boolean isTypeBit, short value1, short value2) {
		
		ByteField a = new ByteField("Name1", value1);
		a.enableTypeOnToBytes(isTypeBit);
		byte[] identifierBits = a.toBytes();

		ByteField c = new ByteField("Name2", value2);
		c.enableTypeOnToBytes(false);
		byte[] origBits = c.toBytes();
		
		boolean expected = (value1 == value2);

		assertEquals(expected,
			ByteField.compareBytes(isTypeBit, identifierBits,origBits));
		
	}

	private void xOffsetShortCompare(boolean isTypeBit, short value1, short value2) {

		ByteField a = new ByteField("Name1", value1);
		a.enableTypeOnToBytes(isTypeBit);
		byte[] identifierBits = a.toBytes();

		byte[] shifts = new byte[identifierBits.length + 1];
		for ( int jh=0; jh< identifierBits.length ; jh++) shifts[jh+1] = identifierBits[jh];

		ByteField b = new ByteField("Name2", value2);
		b.enableTypeOnToBytes(false);
		byte[] origBits = b.toBytes();

		boolean expected = (value1 == value2);

		assertEquals(expected,
				 ByteField.compareBytes(isTypeBit,1,shifts, origBits) );
	}
	

	private void xByteBufferShortCompare(boolean isTypeBit, short value1, short value2) {

		ByteField c = new ByteField("A", value1);
		c.enableTypeOnToBytes(isTypeBit);
		byte[] origBits = c.toBytes();
		
		ByteBuffer bb = ByteBuffer.allocate(2);
		bb.putShort(value2);

		boolean expected = (value1 == value2);

		assertEquals(expected,
		ByteField.compareBytes(isTypeBit, origBits, bb.array()) );
	}	
	
	public void testMatchingByte(Byte a) {
		xNativeByteCompare(true, a, a);
	}
	
	private void xNativeByteCompare(boolean isTypeBit, byte value1, byte value2) {
		
		ByteField a = new ByteField("Name1", value1);
		a.enableTypeOnToBytes(isTypeBit);
		byte[] identifierBits = a.toBytes();

		ByteField c = new ByteField("Name2", value2);
		c.enableTypeOnToBytes(false);
		byte[] origBits = c.toBytes();
		
		boolean expected = (value1 == value2);

		assertEquals(expected,
			ByteField.compareBytes(isTypeBit, identifierBits,origBits));
		
	}	
}
