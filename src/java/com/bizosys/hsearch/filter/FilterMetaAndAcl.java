package com.bizosys.hsearch.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FilterMetaAndAcl {
	 
	public AccessStorable userAcl;
	public byte[] keyword = null; //[search in tags]
	public byte[] state = null;
	public byte[] tenant = null;
	public long createdBefore = -1;
	public long createdAfter = -1;
	public long modifiedBefore = -1;
	public long modifiedAfter = -1;
	 
	public byte[] bytesA = null;
	 
	public FilterMetaAndAcl() {
	}
	 
	/**
	 * The client creates this which eventually stores as a byte array
	 * @param keyword
	 * @param state
	 * @param tenant
	 * @param createdBefore
	 * @param createdAfter
	 * @param modifiedBefore
	 * @param modifiedAfter
	 */
	public FilterMetaAndAcl(AccessStorable viewAcls, 
		byte[] keyword, byte[] state ,byte[] tenant,
		long createdBefore, long createdAfter,long modifiedBefore, long modifiedAfter ) {

		boolean hasAcl = ( null != viewAcls);
		boolean hasKeyword = ( null != keyword);
		boolean hasState = ( null != state);
		boolean hasTenant = ( null != tenant);
		boolean hasCB = ( -1 != createdBefore);
		boolean hasCA = ( -1 != createdAfter);
		boolean hasMB = ( -1 != modifiedBefore);
		boolean hasMA = ( -1 != modifiedAfter);
		
		byte filterFlag = bitsToByte(new boolean[]{
			hasAcl, hasKeyword, hasState,hasTenant,hasCB,hasCA,hasMB,hasMA});
		
		int totalBytes = 1;
		byte[] aclB = ( hasAcl) ? viewAcls.toBytes() : null;
		if ( hasAcl) totalBytes = totalBytes + aclB.length + 2;
		if ( hasKeyword ) totalBytes = totalBytes + keyword.length + 2;
		if ( hasState ) totalBytes = totalBytes + state.length + 2;
		if ( hasTenant ) totalBytes = totalBytes + tenant.length + 2;
		if ( hasCB ) totalBytes = totalBytes + 8;
		if ( hasCA ) totalBytes = totalBytes + 8;
		if ( hasMB ) totalBytes = totalBytes + 8;
		if ( hasMA ) totalBytes = totalBytes + 8;
		
		byte[] bytes = new byte[totalBytes];
		int index=0;
		bytes[index++] = filterFlag;
		if ( hasAcl ) index = writeBytes(aclB, bytes, index);
		if ( hasKeyword ) index = writeBytes(keyword, bytes, index);
		if ( hasState ) index = writeBytes(state, bytes, index);
		if ( hasTenant ) index = writeBytes(tenant, bytes, index);
		if ( hasCB ) index = writeLong(createdBefore, bytes, index);
		if ( hasCA ) index = writeLong(createdAfter, bytes, index);
		if ( hasMB ) index = writeLong(modifiedBefore, bytes, index);
		if ( hasMA ) index = writeLong(modifiedAfter, bytes, index);
		
		this.bytesA = bytes;
	}

	private int writeLong(long variable, byte[] bytes, int index) {
		System.arraycopy(putLong(variable),0, bytes, index, 8);
		index = index + 8;
		return index;
	}

	private int writeBytes(byte[] variableB, byte[] bytes, int index) {
		short variableLen = (short) variableB.length;
		bytes[index++] = (byte)(variableLen >> 8 & 0xff);
		bytes[index++] = (byte)(variableLen & 0xff);
		System.arraycopy(variableB, 0, bytes, index, variableLen);
		index = index + variableLen;
		return index;
	}

	public void writeHeader(DataOutput out) throws IOException {
		out.write(this.bytesA.length);
		out.write(this.bytesA);
	}
	 
	public void readHeader(DataInput in) throws IOException {
		int totalB = in.readInt();
		System.out.println("Total Bytes:" + totalB);
		this.bytesA = new byte[totalB];
		in.readFully(this.bytesA, 0, totalB);
		deserialize();
	}

	public void deserialize() {
		int index=0;
		byte filterFlag = this.bytesA[index++];
		boolean[] filterFlags =byteToBits(filterFlag);
		byte counter = 0;
		
		if ( filterFlags[counter++]) {
			short len = getShort(index, this.bytesA);
			index = index + 2;
			this.userAcl = new AccessStorable(this.bytesA, index, len);
			index = index + len;
		}
		
		if ( filterFlags[counter++] ) {
			short len = getShort(index, this.bytesA);
			this.keyword = new byte[len];
			index = index + 2;
			System.arraycopy(this.bytesA, index, this.keyword, 0, len);
			index = index + len;
		}

		if ( filterFlags[counter++] ) {
			short len = getShort(index, this.bytesA);
			this.state = new byte[len];
			index = index + 2;
			System.arraycopy(this.bytesA, index, this.state, 0, len);
			index = index + len;
		}

		if ( filterFlags[counter++] ) {
			short len = getShort(index, this.bytesA);
			this.tenant = new byte[len];
			index = index + 2;
			System.arraycopy(this.bytesA, index, this.tenant, 0, len);
			index = index + len;
		}

		if ( filterFlags[counter++]) {
			this.createdBefore = getLong(index, this.bytesA);
			index = index + 8;
		}
		
		if ( filterFlags[counter++]) {
			this.createdAfter = getLong(index, this.bytesA);
			index = index + 8;
		}
		
		if ( filterFlags[counter++]) {
			this.modifiedBefore = getLong(index, this.bytesA);
			index = index + 8;
		}

		if ( filterFlags[counter++]) {
			this.modifiedAfter = getLong(index, this.bytesA);
			index = index + 8;
		}
	}	
	
	/**
	 * Never filter ACL is the information is not available
	 * @param value
	 * @return
	 */
	public boolean allowAccess( byte[] value)  {
		if ( null == value ) return true;
		if ( null == this.userAcl) return false;
		short len = getShort(0, value);
		AccessStorable docAcls = new AccessStorable(value,2,len);
		for (Object docAclO : docAcls) {
			if (compareBytes(0, (byte[]) docAclO, Access.ANY_BYTES)) {
				return true;
			}
			
			for (Object userAcl : this.userAcl) {
				if ( compareBytes(0, (byte[]) docAclO,
					(byte[]) userAcl) ) return true;
			}
		}
		return false;
	}
	
	/**
	 * Filter meta fields based on user supplied filtering criteria
	 * @param storedB
	 * @return
	 */
	public boolean allowMeta(byte[] storedB) {
		
		int pos = 0;
		byte docTypeLen = storedB[pos++];
		pos = pos + docTypeLen;
		
		byte stateLen = storedB[pos++];
		if ( null != state) {
			if ( ! compareBytes(pos, storedB, state) ) return false;
		}
		pos = pos + stateLen;
		byte tenantLen = storedB[pos++];
		if ( null != tenant) {
			if ( ! compareBytes(pos, storedB, tenant) ) return false;
		}
		pos = pos + tenantLen;
		
		byte geoLen = storedB[pos++];
		pos = pos + geoLen;
		
		byte flag_1B = storedB[pos++];
		boolean[] flag_1 = byteToBits(flag_1B);
		
		byte flag_2B = storedB[pos++];
		boolean[] flag_2 = byteToBits(flag_2B);
		
		int bitPos = 0;
		if ( flag_1[bitPos++]) pos = pos+ 4; /** Eastering */
		if ( flag_1[bitPos++]) pos = pos+ 4; /** Northing */
		if ( flag_1[bitPos++]) pos = pos+ 4; /** Weight */
		if ( flag_1[bitPos++]) pos = pos+ 4; /** IP House */
		bitPos = bitPos+ 2; /** Security and Sentiment */
		if ( flag_1[bitPos++]) {  /** Tags Available*/
			short len = getShort(pos, storedB);
			pos = pos + 2;
			if ( null != this.keyword ) {
				if (this.keyword.length > len ) return false;
				if ( -1 == indexOf(storedB, pos, pos+len,
					this.keyword, 0, this.keyword.length) ) return false;
			}
			pos = pos+ len;
		} else {
			if ( null != this.keyword ) return false; //No tags found
		}
		//Social text, No need to read
		bitPos = 0;
		if (flag_2[bitPos++]) {
			long createdOn =getLong(pos, storedB);
			pos = pos+ 8;
			if ( -1 != createdBefore && createdBefore > createdOn) return false;
			if ( -1 != createdAfter && createdAfter < createdOn ) return false;
		}
		
		if (flag_2[bitPos++]) {
			long modifiedOn =getLong(pos, storedB);
			pos = pos+ 8;
			if ( -1 != modifiedBefore && modifiedBefore > modifiedOn) return false;
			if ( -1 != modifiedAfter && modifiedAfter < modifiedOn) return false;
		}
		return true;
	}
	
	public static boolean compareBytes(int offset, 
		byte[] inputBytes, byte[] compareBytes) {
		
		int compareBytesT = compareBytes.length;
		if ( (offset + compareBytesT) > inputBytes.length ) return false;
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
		default:
			compareBytesT--;
			for ( int i=0; i< compareBytesT; i++) {
				if ( compareBytes[i] != inputBytes[offset + i]) return false;
			}
		}
		return true;
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

    public static byte bitsToByte(boolean[] bits) {
		int value = 0;
        for (int i = 0; i < 8; i++) {
			if(bits[i] == true) {
				value = value | (1 << i);
			}
        }
        return (byte)value;
	}		

    public static final boolean[] byteToBits(byte b) {
        boolean[] bits = new boolean[8];
        for (int i = 0; i < bits.length; i++) {
            bits[i] = ((b & (1 << i)) != 0);
        }
        return bits;
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
    
    static int indexOf(byte[] source, int startPosition, int endPosition,
    		byte[] target, int targetOffset, int targetCount ) {	
    	
    	byte first  = target[targetOffset];
    	int i = startPosition;
    	startSearchForFirstChar: 
		while (true) {	
			while (i <= endPosition && source[i] != first)i++;
			if (i > endPosition) return -1;
			int j = i + 1;
			int end = j + targetCount - 1;
			int k = targetOffset + 1;
			while (j < end) {
				if (source[j++] != target[k++]) {
					i++;
					continue startSearchForFirstChar;
				}
			}
			return i - startPosition;
		}
	}
}
