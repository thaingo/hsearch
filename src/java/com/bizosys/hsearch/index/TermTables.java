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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.bizosys.hsearch.common.IStorable;
import com.bizosys.hsearch.common.RecordScalar;
import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.hbase.HbaseLog;
import com.bizosys.hsearch.hbase.HReader;
import com.bizosys.hsearch.hbase.HWriter;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.hbase.NVBytes;
import com.bizosys.hsearch.inpipe.SaveToIndexRecord;
import com.bizosys.hsearch.schema.ILanguageMap;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.oneline.SystemFault;

/**
 * Multiple term families grouped inside a Termtable
 * @author karan
 *
 */
public class TermTables {
	
	private static final byte[] BUCKET_COUNTER_BYTES = "BUCKET_COUNTER".getBytes();
	static {init();}
	
	public IStorable bucketId = null;
	public Map<Character, TermFamilies> tables = null;
	
	public TermTables() {
	}
	
	public TermTables(IStorable bucketId) {
		this.bucketId = bucketId;
	}
	
	public void add(Term aTerm, ILanguageMap lang) {
		if ( null == tables) tables = new HashMap<Character, TermFamilies>();
		
		Character table = lang.getTableName(aTerm.term);
		TermFamilies block = null;
		if ( tables.containsKey(table)) block  = tables.get(table);
		else {
			block = new TermFamilies();
			tables.put(table, block);
		}
		block.add(aTerm, lang);
	}
	
	public boolean add(TermTables another) {
		if ( null == another.bucketId) return false;
		
		byte[] anotherPK = another.bucketId.toBytes();
		if ( !Storable.compareBytes(this.bucketId.toBytes(), anotherPK) ) return false;
		
		/**
		 * Both belong to same bucket zone
		 */
		for (Character otherTable : another.tables.keySet()) {
			TermFamilies otherFamilies = another.tables.get(otherTable);
			
			if (this.tables.containsKey(otherTable)) {
				TermFamilies thisFamilies = this.tables.get(otherTable);
				thisFamilies.add(otherFamilies);
			} else {
				this.tables.put(otherTable, otherFamilies);
			}
		}
		return true;
	}

	public void assignDocumentPosition(int docPos) {
		if ( null == tables) return;
		for ( TermFamilies tf : tables.values()) {
			if ( null == tf ) continue;
			tf.assignDocumentPosition(docPos);
		}
	}
	
	public void persist(boolean merge) throws SystemFault {
		try {
			for ( Character tableName : tables.keySet()) {
				TermFamilies termFamilies = tables.get(tableName);
				SaveToIndexRecord record = new SaveToIndexRecord(bucketId);
				record.setTermFamilies(termFamilies); 
				if  (HbaseLog.l.isDebugEnabled()) 
					HbaseLog.l.debug("TermTables.persist Table " + tableName + record.toString());
				HWriter.merge(tableName.toString(), record);
			}
		} catch (Exception ex) {
			throw new SystemFault(ex);
		}
	}
	
	/**
	 * Populates the existing value.
	 * @param tableName
	 * @param termFamilies
	 * @throws SystemFault
	 */
	public void setExistingValue(String tableName, 
		TermFamilies termFamilies) throws SystemFault {
		
		List<NVBytes> existingB = 
			HReader.getCompleteRow(tableName, bucketId.toBytes());
		if ( null == existingB) return;
		
		for (char family: termFamilies.families.keySet()) {
			TermColumns cols = termFamilies.families.get(family);
			for (char col : cols.columns.keySet()) {
				TermList terms = cols.columns.get(col);
				
				for (NVBytes bytes : existingB) {
					if ( bytes.family[0] == family && bytes.name[0] == col) {
						terms.setExistingBytes(bytes.data);
						break;
					}
				}
			}
		}
	}
	
	/**
	 * Get the Running bucket Id
	 */
	public static long getCurrentBucketId() throws SystemFault {
		HbaseLog.l.info("TermTables > aquiring the running bucket.");
		
		NV nv = new NV(IOConstants.NAME_VALUE_BYTES,IOConstants.NAME_VALUE_BYTES);
		RecordScalar scalar = new RecordScalar(BUCKET_COUNTER_BYTES, nv); 
		HReader.getScalar(IOConstants.TABLE_CONFIG,scalar);
		long currentBucket = Storable.getLong(0, nv.data.toBytes());

		if ( HbaseLog.l.isInfoEnabled()) 
			HbaseLog.l.info("TermTables > Running bucket = " + currentBucket);
		return currentBucket;
	}
	
	
	/**
	 * This creates bucket Id, unique across machines.
	 * @return	The bucket Id
	 * @throws SystemFault
	 */
	public static long createBucketId() throws SystemFault {
		
		HbaseLog.l.debug("TermBucket > Creating a new bucket Zone");
		
		/**
		 * Get next bucket Id
		 */
		NV nv = new NV(IOConstants.NAME_VALUE_BYTES,IOConstants.NAME_VALUE_BYTES);
		RecordScalar scalar = new RecordScalar(BUCKET_COUNTER_BYTES, nv); 
		long bucketId = HReader.generateKeys(IOConstants.TABLE_CONFIG,scalar,1);

		/**
		 * Put the bucket as a row for counting document serials. 
		 */
		HbaseLog.l.debug("TermBucket > Setting serial counter for this bucket :" + bucketId);

		long startPos = Short.MIN_VALUE;
		nv.data = new Storable(startPos);
		RecordScalar docSerial = new RecordScalar(
			Storable.putLong(bucketId), nv); 
		try {
			HWriter.insertScalar(IOConstants.TABLE_CONFIG, docSerial);
			HbaseLog.l.info("TermBucket > Bucket setup completed :" + bucketId);
			return bucketId;
		} catch (IOException ex) {
			HbaseLog.l.fatal("TermBucket > Setting serial counter Failed:" + bucketId, ex);
			throw new SystemFault(ex);
		}
	}
	
	/**
	 * This create document serial no inside a bucket id, unique across machines
	 * @param bucketId	The current bucket id
	 * @param amount	Amount of documents to be added
	 * @return	Moved document serial position
	 * @throws SystemFault
	 * @throws BucketIsFullException
	 */
	public static short createDocumentSerialIds(long bucketId, int amount) 
		throws SystemFault, BucketIsFullException {
		
		/**
		 * Generate Ids for this bucket
		 */
		
		HbaseLog.l.debug("Generating buckets keys");
		NV nv = new NV(IOConstants.NAME_VALUE_BYTES,IOConstants.NAME_VALUE_BYTES);
		byte[] pkBucketId = Storable.putLong(bucketId);
		RecordScalar scalar = new RecordScalar(pkBucketId, nv);
		long bucketMaxPos =  
			HReader.generateKeys(IOConstants.TABLE_CONFIG,scalar,amount);
		HbaseLog.l.debug("Buckets keys generated :" + bucketMaxPos);

		int maxValue = Short.MAX_VALUE - Short.MIN_VALUE;
		if (  bucketMaxPos >= maxValue) {
			HbaseLog.l.warn("Crossed the bucket limit of storage :" + bucketMaxPos);
			BucketIsFullException bife = new BucketIsFullException(bucketMaxPos);
			throw bife;
		}
		return new Long(bucketMaxPos).shortValue();
	}
	
	/**
	 * This gives all the rows from all tables.
	 * @param bucketId	Bucket Id
	 * @return	List of name-value bytes
	 * @throws SystemFault
	 */
	public static List<NVBytes> get(long bucketId) throws SystemFault {
		List<NVBytes> allFields = null; 
		for (Character c : ILanguageMap.ALL_TABLES) {
			List<NVBytes> nvs = HReader.getCompleteRow(c.toString(),Storable.putLong(bucketId));
			if ( null == allFields) allFields = nvs;
			if ( null != nvs) allFields.addAll(nvs);
		}
		return allFields;
	}
	
	/**
	 * Initializes the term buckets
	 * Initial System: There will be no bucket. Start from Long.MIN_VALUE
	 * Second time onwards : Continue 
	 */
	public static void init() {
		try {
			NV nv = new NV(IOConstants.NAME_VALUE_BYTES,IOConstants.NAME_VALUE_BYTES);
			if ( ! HReader.exists(IOConstants.TABLE_CONFIG, BUCKET_COUNTER_BYTES)) {
				HbaseLog.l.info("Bucket Counter setup is not there. Setting up bucket id counter.");
				RecordScalar bucketCounter = new RecordScalar(new Storable(BUCKET_COUNTER_BYTES), nv);
				nv.data = new Storable(Long.MIN_VALUE);
				HWriter.insertScalar(IOConstants.TABLE_CONFIG, bucketCounter);
				HbaseLog.l.info("Bucket Counter setup is complete.");
			}
		} catch (IOException ex) {
			HbaseLog.l.fatal("TermBucket > Bucker Bucket Counter Creation Failure:", ex);
			System.exit(1);
		} catch (SystemFault ex) {
			HbaseLog.l.fatal("TermBucket > Bucker Bucket Counter Creation Failure:", ex);
			System.exit(1);
		}
	}
}