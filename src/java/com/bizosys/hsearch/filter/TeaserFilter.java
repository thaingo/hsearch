package com.bizosys.hsearch.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;

public class TeaserFilter implements Filter {
	private static final byte TEASER_BYTE = "c".getBytes()[0];
	private static final int SECTION_LENGTH = 360;
	
	byte[][] bWords = null;

	public TeaserFilter(){}
	
	public TeaserFilter(byte[][] bWords){
		this.bWords = bWords;
	}
	
	public boolean filterAllRemaining() {
		return false;
	}

	public boolean filterRow() {
		return false;
	}

	/**
	 * last chance to drop entire row based on the sequence of filterValue() 
	 * calls. Eg: filter a row if it doesn't contain a specified column
	 */
	public void filterRow(List<KeyValue> kvL) {
		if ( null == kvL) return;
		if ( 0 == kvL.size()) return;
		Iterator<KeyValue> kvItr = kvL.iterator();
	
		byte[] data = null;
		KeyValue kv = null;
		for ( int i=0; i< kvL.size(); i++ ) {
			kv = kvItr.next();
			if (TEASER_BYTE == kv.getQualifier()[0]) {
				BuildTeaserHighlighter bth = new BuildTeaserHighlighter();
				bth.find(kv.getValue(),bWords,SECTION_LENGTH);
				break;
			}
		}
		if ( null != data ) {
			kvItr.remove();
			kvL.add(new KeyValue(kv.getRow(),
				kv.getFamily(), kv.getQualifier(), data));
		}
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
		int len = in.readByte();
		int index = 1;
		this.bWords = new byte[len][];
		
		for ( int i=0; i<len; i++ ) {
			int wLen = in.readByte() ;
			index++;
			this.bWords[i] = new byte[wLen];
			in.readFully(this.bWords[i], index, wLen);
			index = index + wLen;
		}
		
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeByte(bWords.length);
		for ( int i=0; i<bWords.length; i++ ) {
			out.writeByte(bWords[i].length);
			out.write(bWords[i]);
		}
	}

	public ReturnCode filterKeyValue(KeyValue arg0) {
		return ReturnCode.INCLUDE;
	}
}
