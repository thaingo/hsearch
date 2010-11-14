package com.bizosys.hsearch.util;

import java.util.ArrayList;
import java.util.List;

import com.bizosys.hsearch.common.IStorable;
import com.bizosys.hsearch.hbase.NV;

/**
 * A Record 
 * @author bizosys
 *
 */
public class Record {
	public IStorable pk = null;
	public List<NV> nvs = null;
	public long checkoutTime = -1L; 

	public Record(IStorable pk, List<NV> kvs ) {
		this.pk = pk;
		this.nvs = kvs;
	}
	
	public Record(IStorable pk, NV kv ) {
		this.pk = pk;
		this.nvs = new ArrayList<NV>(1);
		nvs.add(kv);
	}	

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("  PK :").append(this.pk);
		if ( null == nvs) return sb.toString();
		for (NV nv : nvs) {
			sb.append(nv.toString()).append('\n');
		}
		return sb.toString();
	}

}
