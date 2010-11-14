package com.bizosys.hsearch.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;

public class TermFilter implements Filter {
	public byte[]  B;
	public byte[]  H;
	private byte[] matchedTLBytes = null;

	public TermFilter(){}
	public TermFilter( byte[] bytes){
		this.B=bytes;
		this.H = new byte[]{B[0],B[1],B[2],B[3]};
	}

	public boolean filterAllRemaining() {
		return false;
	}

	/**
	 *  true to drop this key/value
	 */
	public ReturnCode filterKeyValue(KeyValue kv) {
		matchedTLBytes = null;
		boolean isMatched = FilterIds.isMatchingBucket(kv.getRow(),B);
		System.out.println("Bucket Matched:"  + isMatched);
		if ( isMatched ) {
			matchedTLBytes = FilterIds.isMatchingColBytes(kv.getValue(), B);
			isMatched = ( null != matchedTLBytes);
			System.out.println("Col Matched:"  + isMatched);
			if (isMatched ) return ReturnCode.INCLUDE;
		}
		return ReturnCode.SKIP;
	}

	public boolean filterRow() {
		return false;
	}

	/**
	 * last chance to drop entire row based on the sequence of filterValue() 
	 * calls. Eg: filter a row if it doesn't contain a specified column
	 */
	public void filterRow(List<KeyValue> kvL) {
		System.out.println("filterRow : ENTER" ) ;
		if ( null == matchedTLBytes) return;
		if ( null == kvL) return;
		if ( 0 == kvL.size()) return;
		KeyValue kv = kvL.get(0);
		kvL.clear();
		System.out.println("filterRow :" + matchedTLBytes.length) ;
		kvL.add(new KeyValue(kv.getRow(),
			kv.getFamily(), kv.getQualifier(), matchedTLBytes));
	}
	
	/**
	 * true to drop this row, if false, we will also call
	 */
	public boolean filterRowKey(byte[] rowKey, int offset, int length) {
		return false;
	}
	
	public KeyValue getNextKeyHint(KeyValue arg0) {
		return null;
	}
	
	public boolean hasFilterRow() {
		return true;
	}
	
	public void reset() {
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int T = FilterIds.readHeader(in);
		this.B = new byte[T];
		in.readFully(this.B, 0, T);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		int BT = B.length;
		FilterIds.writeHeader(out, BT);
		out.write(B);
	}
}
