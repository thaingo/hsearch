package com.bizosys.hsearch.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class FilterIds {
	public static final int KEYWORD_BYTES = 5;
	
	/**
	 * After the Top 4 all others are continuous bucket Ids
	 * @param rowKey
	 * @param offset
	 * @param length
	 * @param inB
	 * @return
	 */
	public static final boolean isMatchingBucket(byte[] rowKey, byte[] inB) {
		
		int inBLen = inB.length;
		if ( 6 >= inBLen) return true; //Only Hash + Typecodes

		for ( int i=6; i<inBLen; i++) {
			System.out.println("Matching Bucket @ " + i);
			if ( inB[i] == rowKey[0] &&
					inB[i+1] == rowKey[1] &&
					inB[i+2] == rowKey[2] &&
					inB[i+3] == rowKey[3] &&
					inB[i+4] == rowKey[4] &&
					inB[i+5] == rowKey[5] &&
					inB[i+6] == rowKey[6] &&
					inB[i+7] == rowKey[7] ) return true;
			i = i+8;		
		}
		return false;
	}
	
	/**
	 * Check for matching col bytes.
	 * @param storeB
	 * @param inB
	 * @return
	 */
	public static final byte[] isMatchingColBytes( byte[] storeB, byte[] inB) {
		if ( null == storeB ) return null;
		if ( null == inB ) return null;
		int inT = inB.length;
		
		int storeL = storeB.length;
		int pos = 0, startPos=0;
		int termsT = 0;
		while ( storeL > pos) {
			//Match Keyword hash
			boolean isMatched = 
				storeB[pos] == inB[0] && 
				storeB[pos+1] == inB[1] &&
				storeB[pos+2] == inB[2] &&
				storeB[pos+3] == inB[3];
			
			pos = pos + 4;
			termsT = (byte) storeB[pos++];
			if ( -1 == termsT) {
				termsT = getInt(pos,storeB );
				pos = pos + 4;
			}

			if ( ! isMatched) { /** Term Has Not Matched */
				pos = pos + (termsT * KEYWORD_BYTES);
				continue;
			}
			
			if ( inT > 4 && Byte.MIN_VALUE != inB[4]) { /** Doc Type code match needed*/
				isMatched = false;
				for (int i=0; i<termsT; i++ ) {
					if ( storeB[pos+i] == inB[4] ) {
						isMatched = true; break;
					}
				}
			}
			
			if ( ! isMatched) { /** Doc Type Has Not Matched */
				pos = pos + (termsT * KEYWORD_BYTES);
				continue;
			}
			
			if ( inT > 5 && Byte.MIN_VALUE != inB[5]) { /** Term  Type code match needed*/
				isMatched = false;
				startPos = pos+termsT;
				for (int i=0; i<termsT; i++ ) {
					if ( storeB[startPos+i] == inB[5] ) {
						isMatched = true; break;
					}
				}
			}
			
			if ( ! isMatched) { /** Term Type Has Not Matched */
				pos = pos + (termsT * KEYWORD_BYTES);
				continue;
			}
			
			/** Keyword, Termtype, Doctype all has Matched */
			int termLstBytesT = (termsT * KEYWORD_BYTES);
			byte[] termLstBytes = new byte[termLstBytesT];
			System.arraycopy(storeB,pos, termLstBytes, 0, termLstBytesT);
			return termLstBytes;
			
		}
		return null;
	}
	
	/**
	 * Reads the header section of input data
	 * @param in
	 * @return
	 * @throws IOException
	 */
	public static final int readHeader(DataInput in) throws IOException {
		int T = (in.readByte() << 24 ) + 
		( (in.readByte() & 0xff ) << 16 ) + 
		(  ( in.readByte() & 0xff ) << 8 ) + 
		( in.readByte() & 0xff );
		return T;
	}	
	
	/**
	 * Write the header seciton of supplied header
	 * @param out
	 * @param BT
	 * @throws IOException
	 */
	public static final void writeHeader(DataOutput out, int BT) throws IOException {
		out.write(new byte[] { (byte)(BT >> 24),
			(byte)(BT >> 16 ),(byte)(BT >> 8 ), (byte)(BT) });
	}
	
	
	/**
	 * Integer - Byte conversion
	 * @param index
	 * @param inputBytes
	 * @return
	 */
	public static final int getInt(int index, byte[] inputBytes) {
		
		int intVal = (inputBytes[index] << 24 ) + 
		( (inputBytes[++index] & 0xff ) << 16 ) + 
		(  ( inputBytes[++index] & 0xff ) << 8 ) + 
		( inputBytes[++index] & 0xff );
		return intVal;
	}
	
}
