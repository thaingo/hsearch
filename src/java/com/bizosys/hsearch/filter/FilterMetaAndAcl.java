package com.bizosys.hsearch.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FilterMetaAndAcl {
	 
	private StorableList editAcls;
	private StorableList viewAcls;
	 
	 /** 
	  * [search in tags], [state], [tenant], 
	  * [created before], [created after],
	  * [modified before], [modified after],
	  */
	 public byte[] keyword = null;
	 public byte[] state = null;
	 public byte[] tenant = null;
	 public long createdBefore = -1;
	 public long createdAfter = -1;
	 public long modifiedBefore = -1;
	 public long modifiedAfter = -1;

	public void readHeader(DataInput in) throws IOException {
		int T = (in.readByte() << 24 ) + 
		( (in.readByte() & 0xff ) << 16 ) + 
		(  ( in.readByte() & 0xff ) << 8 ) + 
		( in.readByte() & 0xff );
	}	
	
	public void writeHeader(DataOutput out) throws IOException {
		out.write(new byte[] { (byte)(BT >> 24),
			(byte)(BT >> 16 ),(byte)(BT >> 8 ), (byte)(BT) });
	}
	 
	public boolean filterAcl( byte[] value)  {
		if ( null == value ) return true;
		
		int pos = 0;
		short len = getShort(pos, value);
		pos = pos + 2;
		if ( editAcls != null && 0 != len ) { /* Edit Acl */
			if ( ! hasAccess(editAcls, new StorableList(value) ) ) return false;
			pos = pos + len;
		}
		
		len = getShort(pos, value);
		pos = pos + 2;
		if ( viewAcls != null && 0 != len ) { /* Edit Acl */
			if ( ! hasAccess(viewAcls, new StorableList(value) ) ) return false; 
		}
		
		return true;
	}
	
	/**
	 * Check for the access rights on the information
	 * @param userAcls
	 * @param access
	 * @return
	 */
	private boolean hasAccess (StorableList userAcls, StorableList access) {

		boolean allow = false;
		for (Object objFoundAcl : access) {
			byte[] foundAcl =  ((byte[]) objFoundAcl);
			
			if (compareBytes(0, foundAcl, Access.ANY_BYTES)) {
				allow = true; break;
			}
			
			for (Object userAcl : userAcls) {
				allow = compareBytes(0, foundAcl, (byte[]) userAcl);
				if ( allow ) break;
			}
			if ( allow ) break;
		}
		return allow;
	}	
	
	/**
	 * Filter meta fields based on user supplied filtering criteria
	 * @param bytes
	 * @return
	 */
	public boolean filterMeta(byte[] bytes) {
		
		int pos = 0;

		byte docTypeLen = bytes[pos];
		pos++;
		pos = pos + docTypeLen;
		
		byte stateLen = bytes[pos];
		pos++;
		if ( null != state) {
			if ( ! compareBytes(pos, bytes, state) ) return false;
		}
		pos = pos + stateLen;
			
		byte tenantLen = bytes[pos];
		pos++;
		if ( null != tenant) {
			if ( ! compareBytes(pos, bytes, tenant) ) return false;
		}
		pos = pos + tenantLen;
		pos = bytes[pos] /** Geo house */ + pos++;
		
		byte flag_1B = bytes[pos++];
		boolean[] flag_1 = byteToBits(flag_1B);
		
		byte flag_2B = bytes[pos++];
		boolean[] flag_2 = byteToBits(flag_2B);
		
		int bitPos = 0;
		if ( flag_1[bitPos++]) pos = pos+ 4; /** Eastering */
		if ( flag_1[bitPos++]) pos = pos+ 4; /** Northing */
		if ( flag_1[bitPos++]) pos = pos+ 4; /** Weight */
		if ( flag_1[bitPos++]) pos = pos+ 4; /** IP House */
		bitPos = bitPos+ 2; /** Security and Sentiment */
		if ( flag_1[bitPos++]) pos = pos+ 4; /** IP House */
		if ( flag_1[bitPos++]) {  /** Tags Available*/
			short len = getShort(pos, bytes);
			pos = pos + 2;
			if ( searchInTag ) {
				//Look for the keyword
				boolean found = false;	
				for ( int i=0; i<len ; i++) {
					found = true;
					for ( int j=0; j<keywordLen; j++) {
						if ( bytes[pos+i+j] != keyword[j]) found = false;
					}
					if ( found) break;
				}
				if ( !found) return false;
			}
			pos = pos+ len;
		} else {
			if ( searchInTag ) return false; //No tags found
		}
		
		if (flag_1[bitPos++]) {
			short len = getShort(pos, bytes);
			pos = pos + 2 + len;
		}
		
		bitPos = 0;
		if (flag_2[bitPos++]) {
			if ( -1 != createdBefore && createdBefore > getLong(pos, bytes) ) return false;
			if ( -1 != createdAfter && createdAfter < getLong(pos, bytes) ) return false;
			pos = pos+ 8;
		}
		
		if (flag_2[bitPos++]) {
			if ( -1 != modifiedBefore && modifiedBefore > getLong(pos, bytes) ) return false;
			if ( -1 != modifiedAfter && modifiedAfter < getLong(pos, bytes) ) return false;
			pos = pos+ 8;
		}
		
		if (flag_2[bitPos++]) {
			if ( -1 != validBefore && validBefore > getLong(pos, bytes) ) return false;
			if ( -1 != validAfter && validAfter < getLong(pos, bytes) ) return false;
			pos = pos+ 8;
		}

		return true;
	}
	
	public static boolean compareBytes(int offset, 
		byte[] inputBytes, byte[] compareBytes) {
		
		int compareBytesT = compareBytes.length;
		if ( compareBytes[0] != inputBytes[offset]) return false;
		if ( compareBytes[compareBytesT - 1] != inputBytes[compareBytesT + offset - 1] ) return false;
		switch (compareBytesT) {
		
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
	
    public static final boolean[] byteToBits(byte b) {
        boolean[] bits = new boolean[8];
        for (int i = 0; i < bits.length; i++) {
            bits[i] = ((b & (1 << i)) != 0);
        }
        return bits;
    }
    
	public static short getShort(int startPos, byte[] inputBytes) {
		return (short) (
			(inputBytes[startPos] << 8 ) + ( inputBytes[++startPos] & 0xff ) );
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
		
	
}
