package com.bizosys.hsearch.util;

import com.bizosys.hsearch.common.IStorable;
import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.hbase.NV;

/**
 * A Record 
 * @author bizosys
 *
 */
public class RecordScalar {
	public IStorable pk = null;
	public NV kv = null;
	public long checkoutTime = -1L; 

	public RecordScalar(IStorable pk, NV kv ) {
		this.pk = pk;
		this.kv = kv;
	}
	
	public RecordScalar(byte[] pk, NV kv ) {
		this.pk = new Storable(pk);
		this.kv = kv;
	}
	
	public String toString() {
		return ("Record = " + this.pk);
	}

}
