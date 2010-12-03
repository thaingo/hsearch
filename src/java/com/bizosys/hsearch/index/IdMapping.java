/*
* Copyright 2010 The Apache Software Foundation
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.bizosys.hsearch.index;

import java.io.IOException;
import java.util.List;

import com.bizosys.hsearch.common.IStorable;
import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.hbase.HReader;
import com.bizosys.hsearch.hbase.HWriter;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.hbase.NVBytes;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.hsearch.util.RecordScalar;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;

public class IdMapping {
	
	public IStorable originalId;
	public Long bucketId;
	public Short docSerialId;
	
	public IdMapping(IStorable originalId,Long bucketId,Short docSerialId ) {
		this.originalId = originalId;
		this.bucketId = bucketId;
		this.docSerialId = docSerialId;
	}
	
	public static IdMapping load(IStorable originalId) 
	throws ApplicationFault, SystemFault{
		
		NV nv = new NV(IOConstants.NAME_VALUE_BYTES, 
				IOConstants.NAME_VALUE_BYTES );
		RecordScalar scalar = new RecordScalar(originalId,nv);
			
		HReader.getScalar(IOConstants.TABLE_IDMAP, scalar);
		if ( null == scalar.kv.data) return null;
		
		String key = new String(scalar.kv.data.toBytes());
		
		int foundAt = key.indexOf('_');
		if ( foundAt == -1) throw new ApplicationFault("IdMapping: Illegal Key:" + key);
		Long bucketId = new Long(key.substring(0,foundAt));
		Short docSerial = new Short(key.substring(foundAt+1));
		return new IdMapping(originalId,bucketId,docSerial);
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
			HWriter.insertScalar(IOConstants.TABLE_IDMAP, records);
		} catch (IOException ex) {
			throw new SystemFault(ex);
		}
	}
	
	public static final void purge(long bucketId, short docSerialId) throws SystemFault{
		try {
			HWriter.delete(IOConstants.TABLE_IDMAP, 
				new Storable(getKey(bucketId, docSerialId)) );
		} catch (IOException ex) {
			throw new SystemFault(ex);
		}
	}
	
	
	public static final List<NVBytes> getKey(byte[] origKey) throws SystemFault{
		return HReader.getCompleteRow(IOConstants.TABLE_IDMAP, origKey);
	}
	
	public static final String getKey(long bucket, short docPos) {
		StringBuilder mappedKey = new StringBuilder(20);
		mappedKey.append(bucket).append('_');
		mappedKey.append(docPos);
		return mappedKey.toString();
	}
	
	public static final long getBucket(String key) throws ApplicationFault {
		int foundAt = key.indexOf('_');
		if ( foundAt == -1) throw new ApplicationFault("IdMapping: Illegal Key:" + key);
		return new Long(key.substring(0,foundAt));
	}
	
	public static final short getDocSerial(String key) throws ApplicationFault {
		int foundAt = key.indexOf('_');
		if ( foundAt == -1) throw new ApplicationFault("IdMapping: Illegal Key:" + key);
		return new Short(key.substring(foundAt+1));
	}

}