package com.bizosys.hsearch.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;

public class PreviewFilter implements Filter {
	private static final byte META_BYTE = "m".getBytes()[0];
	private static final byte ACL_BYTE = "a".getBytes()[0];
	FilterMetaAndAcl fma = null;
	byte[] bytes = null;

	public PreviewFilter(){}
	
	public PreviewFilter(FilterMetaAndAcl fma){
		this.fma = fma;
	}
	
	public FilterMetaAndAcl getFma(){
		return this.fma;
	}
	public void setFma(FilterMetaAndAcl fma) {
		this.fma = fma;
	}

	public boolean filterAllRemaining() {
		return false;
	}

	/**
	 *  true to drop this key/value
	 */
	public ReturnCode filterKeyValue(KeyValue kv) {
		if ( ACL_BYTE == kv.getQualifier()[0]) { // Match ACL
			if ( ! this.fma.filterAcl(kv.getValue())) return ReturnCode.SKIP;
		} else if (META_BYTE == kv.getQualifier()[0]) {
			if ( ! this.fma.filterMeta(kv.getValue())) return ReturnCode.SKIP;
		}
		return ReturnCode.INCLUDE;
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
		for ( int i=0; i< kvL.size(); i++ ) {
			KeyValue kv = kvItr.next();
			if (ACL_BYTE == kv.getQualifier()[0]) kvItr.remove();
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
		if ( null == this.fma ) this.fma = new FilterMetaAndAcl();
		this.fma.readHeader(in);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		if ( null == this.fma ) this.fma = new FilterMetaAndAcl();
		fma.writeHeader(out);
	}
}
