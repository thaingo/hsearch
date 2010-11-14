package com.bizosys.hsearch.index;

import java.io.IOException;
import java.util.List;

import org.apache.oneline.SystemFault;

import com.bizosys.hsearch.common.IStorable;
import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.hbase.HWriter;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.hsearch.util.RecordScalar;

public class IdMapping {
	
	public IStorable originalId;
	public Long bucketId;
	public Short docSerialId;
	
	public IdMapping(IStorable originalId,Long bucketId,Short docSerialId ) {
		this.originalId = originalId;
		this.bucketId = bucketId;
		this.docSerialId = docSerialId;
	}
	
	public void build(List<RecordScalar> records) {
		
		NV nv = new NV(IOConstants.NAME_VALUE_BYTES, 
			IOConstants.NAME_VALUE_BYTES, 
			new Storable(getKey(this.bucketId, this.docSerialId) ) );
		
		RecordScalar record = new RecordScalar(this.originalId,nv);
		records.add(record);
	}
	
	public static final void persist(List<RecordScalar> records) throws SystemFault{
		try {
			HWriter.insertScalar(IOConstants.TABLE_IDMAP, records, true);
		} catch (IOException ex) {
			throw new SystemFault(ex);
		}
	}

	public static final String getKey(long bucket, short docPos) {
		StringBuilder mappedKey = new StringBuilder(20);
		mappedKey.append(bucket).append('_');
		mappedKey.append(docPos);
		return mappedKey.toString();
	}
}