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

public class ByteField extends Storable {

	public static final ByteField[] EMPTY_BYTEFIELDS = new ByteField[]{};
	public String name = null;
	public byte[] nameBytes = null;
	public boolean isTypeOnToBytes = false;
	
	public ByteField(String name, byte[] origVal) {
		super(origVal);
		this.name = name;
	}

	public ByteField(String name, byte[] inputBytes, byte type) {
		super(inputBytes, type);
		this.name = name;
	}
	
	public ByteField(String name, byte[] inputBytes, byte type, int startPos) {
		super(inputBytes, type,startPos);
		this.name = name;
	}

	public ByteField(String name, byte[] inputBytes, byte type, 
	int startPos, int endPos ) {
		
		super(inputBytes, type,startPos,endPos);
		this.name = name;
	}
	
	public ByteField(String name, IStorable storable) {
		super(storable);
		this.name = name;
	}
	
	public ByteField(String name, String origVal) {
		super(origVal);
		this.name = name;
	}

	public ByteField(String name, Byte origVal) {
		super(origVal);
		this.name = name;
	}
	
	public ByteField(String name, Short origVal) {
		super(origVal);
		this.name = name;
	}
	
	public ByteField(String name, Integer origVal) {
		super(origVal);
		this.name = name;
	}
	
	public ByteField(String name, Long origVal) {
		super(origVal);
		this.name = name;
	}
	
	public ByteField(String name, Float origVal) {
		super(origVal);
		this.name = name;
	}
	
	public ByteField(String name, Double origVal) {
		super(origVal);
		this.name = name;
	}
	
	public ByteField(String name, Boolean origVal) {
		super(origVal);
		this.name = name;
	}
	
	public ByteField(String name, Character origVal) {
		super(origVal);
		this.name = name;
	}

	public ByteField(String name, java.util.Date origVal) {
		super(origVal);
		this.name = name;
	}
	
	public ByteField(String name, java.sql.Date origVal) {
		super(origVal);
		this.name = name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public byte[] getName() {
		if (null == nameBytes) nameBytes = Storable.putString(name);
		return nameBytes;
	}
	
	public void enableTypeOnToBytes(boolean status) {
		this.isTypeOnToBytes = status;
	}
	
	
	@Override
	public byte[] toBytes() {
		if ( this.isTypeOnToBytes ) {
			int byteT = this.byteValue.length; 
			byte[] bytesWithType = new byte[byteT + 1]; 
			System.arraycopy(this.byteValue, 0, bytesWithType, 0, byteT);
			bytesWithType[byteT] = this.type;
			return bytesWithType;
		} else {
			return this.byteValue;
		}
	}

	public static ByteField wrap( byte[] inputBytes) {
		
		if ( null == inputBytes ) return null;
		int inputBytesT = inputBytes.length;
		if ( 1 < inputBytesT ) return null;
		
		byte typeIdentifier = inputBytes[inputBytesT - 1];
		return new ByteField(
			null, inputBytes, typeIdentifier, 0, inputBytesT - 1);
	}
	
	public static ByteField wrap( byte[] name, byte[] inputBytes) {
		
		if ( null == inputBytes ) return null;
		int inputBytesT = inputBytes.length;
		if ( inputBytesT < 1) return null;
		
		byte typeIdentifier = inputBytes[inputBytesT - 1];
		return new ByteField( new String(name),
			inputBytes, typeIdentifier, 0, inputBytesT - 1);
	}
	
	
	public static boolean compareBytes(boolean isTypeBit, int offset, 
			byte[] inputBytes, byte[] compareBytes) {

		int inputBytesT = inputBytes.length;
		int compareBytesT = compareBytes.length;
		if ( isTypeBit ) {
			if ( compareBytesT + 1 !=  inputBytesT - offset) return false;
		} else {
			if ( compareBytesT !=  inputBytesT - offset) return false;
		}
		
		if ( compareBytes[0] != inputBytes[offset]) return false;
		if ( compareBytes[compareBytesT - 1] != inputBytes[compareBytesT + offset - 1] ) return false;
		
		switch (compareBytesT)
		{
			case 3:
				return compareBytes[1] == inputBytes[1 + offset];
			case 4:
				return compareBytes[1] == inputBytes[1 + offset] && 
					compareBytes[2] == inputBytes[2 + offset];
			case 5:
				return compareBytes[1] == inputBytes[1+ offset] && 
					compareBytes[2] == inputBytes[2+ offset] && 
					compareBytes[3] == inputBytes[3+ offset];
			case 6:
				return compareBytes[1] == inputBytes[1+ offset] && 
				compareBytes[3] == inputBytes[3+ offset] && 
				compareBytes[2] == inputBytes[2+ offset] && 
				compareBytes[4] == inputBytes[4+ offset];
			case 7:
			case 8:
			case 9:
			case 10:
			case 11:
			case 12:
			case 13:
			case 14:
			case 15:
			case 16:
			case 17:
			case 18:
			case 19:
			case 20:
			case 21:
			case 22:
			case 23:
			case 24:
			case 25:
			case 26:
			case 27:
			case 28:
			case 29:
			case 30:
				for ( int i=offset; i< compareBytesT - 1; i++) {
					if ( compareBytes[i] != inputBytes[offset + i]) return false;
				}
				break;
				
			case 31:
				
				for ( int a = 1; a <= 6; a++) {
					if ( ! 
					(compareBytes[a] == inputBytes[a+offset] && 
					compareBytes[a+6] == inputBytes[a+6+offset] && 
					compareBytes[a+12] == inputBytes[a+12+offset] && 
					compareBytes[a+18] == inputBytes[a+18+offset] && 
					compareBytes[a+24] == inputBytes[a+24+offset]) ) return false;
				}
				break;
			default:

				for ( int i=offset; i< compareBytesT - 1; i++) {
					if ( compareBytes[i] != inputBytes[offset + i]) return false;
				}
		}
		return true;
	}

	
	public static boolean compareBytes(boolean isTypeBit, byte[] inputBytes, byte[] compareBytes) {
		if ( null == inputBytes) return false;
		int inputBytesT = inputBytes.length;
		int compareBytesT = compareBytes.length;
		if ( isTypeBit ) {
			if ( compareBytesT + 1 !=  inputBytesT) return false;
		} else {
			if ( compareBytesT !=  inputBytesT) return false;
		}
		
		
		if ( compareBytes[0] != inputBytes[0]) return false;
		if ( compareBytes[compareBytesT - 1] != inputBytes[compareBytesT - 1] ) return false;
		
		switch (compareBytesT)
		{
			case 3:
				return compareBytes[1] == inputBytes[1];
			case 4:
				return compareBytes[1] == inputBytes[1] && 
					compareBytes[2] == inputBytes[2];
			case 5:
				return compareBytes[1] == inputBytes[1] && 
					compareBytes[2] == inputBytes[2] && 
					compareBytes[3] == inputBytes[3];
			case 6:
				return compareBytes[1] == inputBytes[1] && 
				compareBytes[3] == inputBytes[3] && 
				compareBytes[2] == inputBytes[2] && 
				compareBytes[4] == inputBytes[4];
			case 7:
			case 8:
			case 9:
			case 10:
			case 11:
			case 12:
			case 13:
			case 14:
			case 15:
			case 16:
			case 17:
			case 18:
			case 19:
			case 20:
			case 21:
			case 22:
			case 23:
			case 24:
			case 25:
			case 26:
			case 27:
			case 28:
			case 29:
			case 30:
				for ( int i=0; i< compareBytesT - 1; i++) {
					if ( compareBytes[i] != inputBytes[i]) return false;
				}
				break;
			case 31:
				for ( int a=1; a <= 6; a++) {
					if ( ! 
					(compareBytes[a] == inputBytes[a] && 
					compareBytes[a+6] == inputBytes[a+6] && 
					compareBytes[a+12] == inputBytes[a+12] && 
					compareBytes[a+18] == inputBytes[a+18] && 
					compareBytes[a+24] == inputBytes[a+24]) ) return false;
				}
				break;
			default:
				
				for ( int i=0; i< compareBytesT - 1; i++) {
					if ( compareBytes[i] != inputBytes[i]) return false;
				}
		}
		return true;
	}
	
	@Override
	public String toString() {
		return "ByteField Name:" + this.name + " , Value = " + this.asIsObject;
	}
}