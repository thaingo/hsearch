package com.bizosys.hsearch.util;

import java.util.List;

import com.bizosys.hsearch.hbase.NVBytes;

/**
 * A Record 
 * @author bizosys
 *
 */
public class RecordBytes {
	public byte[] pk = null;
	public List<NVBytes> nvs = null;

	public RecordBytes(byte[] pk, List<NVBytes> nvs ) {
		this.pk = pk;
		this.nvs = nvs;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("  PK :").append(this.pk);
		if ( null == nvs) return sb.toString();
		for (NVBytes nv : nvs) {
			sb.append(nv.toString()).append('\n');
		}
		return sb.toString();
	}

}
