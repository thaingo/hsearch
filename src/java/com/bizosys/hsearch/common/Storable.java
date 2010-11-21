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

import java.io.UnsupportedEncodingException;

public class Storable implements IStorable {

	public static final byte BYTE_UNKNOWN = -1;
	public static final byte BYTE_STRING = 2;
	public static final byte BYTE_BYTE = 3;
	public static final byte BYTE_SHORT = 4;
	public static final byte BYTE_INT = 5;
	public static final byte BYTE_LONG = 6;
	public static final byte BYTE_BOOLEAN = 7;
	public static final byte BYTE_CHAR = 8;
	public static final byte BYTE_FLOAT = 9;
	public static final byte BYTE_DOUBLE = 10;
	public static final byte BYTE_DATE = 11;
	public static final byte BYTE_SQLDATE = 12;
	public static final byte BYTE_STORABLE = 13;
	
	protected Object asIsObject = null;
	protected byte[] byteValue = null;
	public byte type = BYTE_UNKNOWN;
	
	public Storable(byte origVal) {
		this.asIsObject = origVal;
		this.byteValue = new byte[] {origVal};
	}

	public Storable(byte[] origVal) {
		this.asIsObject = origVal;
		this.byteValue = origVal;
	}
	
	public Storable(byte[] inputBytes, byte type) {
		if ( null != inputBytes) 
			setValueWithParsing(inputBytes, type, 0, inputBytes.length);
	}
	
	public Storable(byte[] inputBytes, byte type, int startPos) {
		this(inputBytes,type,startPos,inputBytes.length);
	}

	public Storable(byte[] inputBytes, byte type, int startPos, int endPos ) {
		if ( null != inputBytes) 
			setValueWithParsing(inputBytes, type, startPos, endPos);
	}

	public Storable(IStorable storable) {
		this.asIsObject = storable;
		if ( null == storable)  this.setValue(BYTE_STORABLE, null);
		else this.setValue(BYTE_STORABLE, storable.toBytes());
	}
	
	public Storable(String origVal) {
		if ( null != origVal) {
			this.asIsObject = origVal;
			try {
				this.setValue(BYTE_STRING, origVal.getBytes("UTF-8"));
			} catch (Exception ex) {
				this.setValue(BYTE_STRING, origVal.getBytes());
			}
		}
	}

	public Storable(Byte origVal) {
		this.asIsObject = origVal;
		this.setValue(BYTE_BYTE, new byte[]{origVal});
	}
	
	public Storable(Short origVal) {
		this.asIsObject = origVal;
		
		short temp = origVal.shortValue();
		this.setValue(BYTE_SHORT, new byte[]{ 
			(byte)(temp >> 8 & 0xff), 
			(byte)(temp & 0xff) });
	}
	
	public Storable(Integer origVal) {
		this.asIsObject = origVal;
		this.setValue(BYTE_INT, putInt(origVal) ) ;
	}
	
	public Storable(Long origVal) {
		this.asIsObject = origVal;
		this.setValue(BYTE_LONG, putLong(origVal) ) ;
	}
	
	public Storable(Float origVal) {
		this.asIsObject = origVal;
		this.setValue(BYTE_FLOAT, putInt(Float.floatToRawIntBits (origVal)) ) ;
	}
	
	public Storable(Double origVal) {
		this.asIsObject = origVal;
		if ( null != origVal ) this.setValue(BYTE_DOUBLE, putLong(Double.doubleToRawLongBits(origVal)) ) ;
	}
	
	public Storable(Boolean bolVal) {
		this.asIsObject = bolVal;
		if ( true == bolVal) this.setValue(BYTE_BOOLEAN, new byte[]{1});
		else this.setValue(BYTE_BOOLEAN, new byte[]{0});
	}
	
	public Storable(Character charVal) {
		this.asIsObject = charVal;
		char temp = charVal.charValue();
		this.setValue(BYTE_CHAR,  new byte[]{ (byte)(temp >> 8 & 0xff), (byte)(temp & 0xff) });
	}

	public Storable(java.util.Date date) {
		this.asIsObject = date;
		if ( null != date ) this.setValue(BYTE_DATE, putLong(date.getTime()) ) ;
	}
	
	public Storable(java.sql.Date date) {
		this.asIsObject = date;
		this.setValue(BYTE_SQLDATE, putLong(date.getTime()) ) ;
	}
	
	public void setValue(byte type, byte[] byteA) {
		if ( null == byteA) return;
		
		this.type = type;
		this.byteValue = byteA;
	}
	
	protected void setValueWithParsing(byte[] inputBytes, byte type, int startPos, int endPos) {
		this.byteValue = inputBytes;
		this.type = type;
		
		switch (type) {
			
			case BYTE_CHAR:
				this.asIsObject = (char) (
					(inputBytes[startPos] << 8 ) +
					( inputBytes[++startPos] & 0xff ) );
				break;
				
			case BYTE_STORABLE:
				int destBytesT = endPos - startPos;
				byte[] bytes = new byte[destBytesT]; 
				System.arraycopy(inputBytes, startPos, bytes, 0, destBytesT);
				this.asIsObject = bytes;
				break;

			case BYTE_STRING:
				int dBytesT = endPos - startPos;
				byte[] value = new byte[dBytesT]; 
				System.arraycopy(inputBytes, startPos, value, 0, dBytesT);
				try {
					this.asIsObject = new String( value , "UTF-8");
				} catch (UnsupportedEncodingException ex) {
					this.asIsObject = new String(value);
				}
				break;

			case BYTE_SHORT:
				this.asIsObject =  getShort(startPos, inputBytes); 
				break;
								
			case BYTE_INT:
				this.asIsObject = getInt(startPos, inputBytes); 
				break;
			
				
			case BYTE_LONG:
				this.asIsObject = getLong(startPos,inputBytes); 
				break;
				
			case BYTE_FLOAT:
				int fVal = getInt(startPos,inputBytes);
				this.asIsObject = Float.intBitsToFloat (fVal);
				break;
			case BYTE_DOUBLE:
				long dVal = getLong(startPos,inputBytes);
				this.asIsObject = Double.longBitsToDouble(dVal);
				break;
				
			case BYTE_DATE:
				long utilDate = getLong(startPos, inputBytes);
				this.asIsObject = new java.util.Date(utilDate);
				break;
				
			case BYTE_SQLDATE:
				long sqldate = getLong(startPos, inputBytes);
				this.asIsObject = new java.sql.Date(sqldate);
				break;
				
			default:
				break;
		}
	}

	
	
	public Object getValue() {
		return this.asIsObject;
	}
	
	public byte[] toBytes() {
		return this.byteValue;
	}
	
	public static boolean compareBytes(int offset, 
	byte[] inputBytes, byte[] compareBytes) {

		int inputBytesT = inputBytes.length;
		int compareBytesT = compareBytes.length;
		if ( compareBytesT !=  inputBytesT - offset) return false;
		
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

	
	public static boolean compareBytes(byte[] inputBytes, byte[] compareBytes) {
		return compareBytes(0,inputBytes,compareBytes);
	}
	
	
	public static boolean compareBytes(char[] inputBytes, char[] compareBytes) {

				int inputBytesT = inputBytes.length;
				int compareBytesT = compareBytes.length;
				if ( compareBytesT !=  inputBytesT) return false;
				
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
					default:
						compareBytesT--;
						for ( int i=0; i< compareBytesT; i++) {
							if ( compareBytes[i] != inputBytes[i]) return false;
						}
				}
				return true;
			}
	

	public static short getShort(int startPos, byte[] inputBytes) {
		return (short) (
			(inputBytes[startPos] << 8 ) + ( inputBytes[++startPos] & 0xff ) );
	}
	
	public static byte[] putShort( short value ) {

		return new byte[] { 
			(byte)(value >> 8 & 0xff), 
			(byte)(value & 0xff) };
	}	
	public static int getInt(int index, byte[] inputBytes) {
		
		int intVal = (inputBytes[index] << 24 ) + 
		( (inputBytes[++index] & 0xff ) << 16 ) + 
		(  ( inputBytes[++index] & 0xff ) << 8 ) + 
		( inputBytes[++index] & 0xff );
		return intVal;
	}
	
	public static byte[] putInt( int value ) {
		return new byte[] { 
			(byte)(value >> 24), 
			(byte)(value >> 16 ), 
			(byte)(value >> 8 ), 
			(byte)(value) }; 
	}
	
	public static long getLong(int index, final byte[] inputBytes) {
		
		if ( 0 == inputBytes.length) return 0;
		
		long longVal = ( ( (long) (inputBytes[index]) )  << 56 )  + 
		( (inputBytes[++index] & 0xffL ) << 48 ) + 
		( (inputBytes[++index] & 0xffL ) << 40 ) + 
		( (inputBytes[++index] & 0xffL ) << 32 ) + 
		( (inputBytes[++index] & 0xffL ) << 24 ) + 
		( (inputBytes[++index] & 0xff ) << 16 ) + 
		( (inputBytes[++index] & 0xff ) << 8 ) + 
		( inputBytes[++index] & 0xff );
		return longVal;
	}
	
	public static byte[] putLong(long value) {
		return new byte[]{
			(byte)(value >> 56), 
			(byte)(value >> 48 ), 
			(byte)(value >> 40 ), 
			(byte)(value >> 32 ), 
			(byte)(value >> 24 ), 
			(byte)(value >> 16 ), 
			(byte)(value >> 8 ), 
			(byte)(value ) };		
	}
	
	public static byte[] putString( String inputObj) {
		try {
			return inputObj.getBytes("UTF-8");
		} catch (UnsupportedEncodingException ex) {
			return inputObj.getBytes();
		}
		
	}
	
	public static String getString(byte[] inputBytes) {
		try {
			return new String( inputBytes , "UTF-8");
		} catch (UnsupportedEncodingException ex) {
			return new String(inputBytes);
		}
	}
	
	public static int getSize(byte type) {
		int size = -1;
		switch(type) {
			case BYTE_SHORT:
				size = Short.SIZE;
				break;
			case BYTE_INT:
				size = Integer.SIZE;
				break;
			case BYTE_LONG:
				size = Long.SIZE;
				break;
			case BYTE_BOOLEAN:
				size = 1;
				break;
			case BYTE_CHAR:
				size = Character.SIZE;
				break;
			case BYTE_FLOAT:
				size = Float.SIZE;
				break;
			case BYTE_DOUBLE:
				size = Double.SIZE;
				break;
			case BYTE_DATE:
				size = Long.SIZE;
				break;
			case BYTE_SQLDATE:
				size = Long.SIZE;
				break;
			default:
				size = -1;
		}
		return size; 
	}
	
    public static final boolean[] byteToBits(byte b) {
        boolean[] bits = new boolean[8];
        for (int i = 0; i < bits.length; i++) {
            bits[i] = ((b & (1 << i)) != 0);
        }
        return bits;
    }

	public static byte bitsToByte(boolean[] bits) {
		return bitsToByte(bits, 0);
    }
	
    public static byte bitsToByte(boolean[] bits, int offset) {
		int value = 0;
        for (int i = 0; i < 8; i++) {
			if(bits[i] == true) {
				value = value | (1 << i);
			}
        }
        return (byte)value;
	}	
	
	@Override
	public String toString() {
		if ( null != this.asIsObject) return this.asIsObject.toString();
		else return "";
	}

}
