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
package com.bizosys.hsearch.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowLock;

import com.bizosys.hsearch.common.IStorable;
import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.util.Record;
import com.bizosys.hsearch.util.RecordScalar;

/**
 * All HBase write calls goes from here.
 * It supports Insert, Delete, Update and Merge operations. 
 * Merge is a operation, where read and write happens inside 
 * a lock. This lock is never exposed to developer.
 * @author karan
 *
 */
public class HWriter {

	/**
	 * Insert just a single scalar record
	 * It goes through transaction in a non batch mode.
	 * @param tableName
	 * @param record
	 * @param nonBatch
	 * @throws IOException
	 */
	public static void insertScalar(String tableName, RecordScalar record) throws IOException {
		if  (HLog.l.isDebugEnabled()) 
			HLog.l.debug("insertScalar 1 : " + tableName);
		
		byte[] pk = record.pk.toBytes();
		Put update = new Put(pk);
		NV kv = record.kv;
		update.add(kv.family.toBytes(),kv.name.toBytes(), kv.data.toBytes());
   		update.setWriteToWAL(true);
		
		HTableWrapper table = null;
		HBaseFacade facade = null;
		try {
			table.put(update);
			table.flushCommits();
		} finally {
			if ( null != facade && null != table) {
				facade.putTable(table);
			}
		}
	}	
	
	/**
	 * Insert multiple scalar records
	 * @param tableName
	 * @param records
	 * @throws IOException
	 */
	public static void insertScalar(String tableName, List<RecordScalar> records) throws IOException {
		if  (HLog.l.isDebugEnabled()) 
			HLog.l.debug("Updating the table " + tableName);
		
		List<Put> updates = null;
		int recordsT = records.size();
		if ( recordsT > 300 ) updates = new Vector<Put>(recordsT); 
		else updates = new ArrayList<Put>(recordsT);
		
		for (RecordScalar record : records) {
			Put update = new Put(record.pk.toBytes());
			NV kv = record.kv;
			update.add(kv.family.toBytes(),kv.name.toBytes(), kv.data.toBytes());
			updates.add(update);
		}
		HTableWrapper table = null;
		HBaseFacade facade = null;
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			table.put(updates);
			table.flushCommits();
		} finally {
			if ( null != facade && null != table) {
				facade.putTable(table);
			}
		}
	}
	
	
	/**
	 * Inserting just a single record, Single column.
	 * As we are inserting there is no need for an check of any modification
	 * from the time we have read..
	 * @param tableName
	 * @param pk
	 * @param nonPkCols
	 * @throws IOException
	 */
	public static void insert(String tableName, Record record) throws IOException {
		byte[] pk = record.pk.toBytes();
		
   		HTableWrapper table = null;
		HBaseFacade facade = null;
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			
			Put update = new Put(pk);
	   		for (NV param : record.getNVs()) {
				update.add(param.family.toBytes(), 
						param.name.toBytes(), param.data.toBytes());
			}	   			
			update.setWriteToWAL(true);				
			table.put(update);
			table.flushCommits();
		} finally {
			if ( null != facade && null != table) {
				facade.putTable(table);
			}
		}
	}
	
	/**
	 * This creates an record in the table
	 * @param tableName
	 * @param record
	 * @throws IOException
	 */
	public static void insert(String tableName, RecordScalar record ) throws IOException {
		if  (HLog.l.isDebugEnabled()) 
			HLog.l.debug("Inserting the table " + tableName);
		
		byte[] pk = record.pk.toBytes();
		Put update = new Put(pk);
		update.add(record.kv.family.toBytes(), 
				record.kv.name.toBytes(), record.kv.data.toBytes());

   		HTableWrapper table = null;
		HBaseFacade facade = null;
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			update.setWriteToWAL(true);
			table.put(update);
			table.flushCommits();
		} finally {
			if ( null != facade && null != table) facade.putTable(table);
		}
	}

	/**
	 * Inserting in batch.
	 * As we are inserting there is no need for an check of any modification
	 * from the time we have read..
	 * @param tableName
	 * @param records
	 * @throws IOException
	 */
	public static void insert(String tableName, List<Record> records) throws IOException {
		if  (HLog.l.isDebugEnabled()) 
			HLog.l.debug("Updating the table " + tableName);
		
		List<Put> updates = null;
		int recordsT = records.size();
		if ( recordsT > 300 ) updates = new Vector<Put>(recordsT); 
		else updates = new ArrayList<Put>(recordsT);
		
		for (Record record : records) {
			Put update = new Put(record.pk.toBytes());
	   		for (NV param : record.getNVs()) {
				update.add(param.family.toBytes(), 
					param.name.toBytes(), param.data.toBytes());
			}
	   		update.setWriteToWAL(true);
			updates.add(update);
		}
		HTableWrapper table = null;
		HBaseFacade facade = null;
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			table.put(updates);
			table.flushCommits();
		} finally {
			if ( null != facade && null != table) {
				facade.putTable(table);
			}
		}
	}
		
	/**
	 * If the record is not found, it will do nothing
	 * @param tableName
	 * @param pk
	 * @param pipe
	 * @throws IOException
	 */
	public static void update(String tableName, 
		byte[] pk, IUpdatePipe pipe) throws IOException {
		
		if ( null == tableName  || null == pk) return;
		/**
		if  (HLog.l.isInfoEnabled()) 
			HLog.l.info("HWriter: update (" + tableName + ") , PK=" + Storable.getLong(0,pk));
		*/

   		HTableWrapper table = null;
		HBaseFacade facade = null;
		RowLock lock = null;
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			Get getter = new Get(pk);
			if ( ! table.exists(getter) ) return;
			lock = table.lockRow(pk);
			Get lockedGet = new Get(pk,lock);
			Put lockedUpdate = new Put(pk,lock);
			Result r = table.get(lockedGet);
			for (KeyValue kv : r.list()) {
				byte[] modifiedB = pipe.process(kv.getFamily(), kv.getQualifier(), kv.getValue());
				KeyValue updatedKV = new KeyValue(r.getRow(), 
					kv.getFamily(), kv.getQualifier(),modifiedB);  
				lockedUpdate.add(updatedKV);
			}
			lockedUpdate.setWriteToWAL(true);
			table.put(lockedUpdate);
			table.flushCommits();
		} finally {
			if ( null != lock) table.unlockRow(lock);
			if ( null != facade && null != table) {
				facade.putTable(table);
			}
		}
	}
	
	/**
	 * Just updates the record. It overrides all previous changes.
	 * @param table
	 * @throws IOException
	 */
	public static void update(String tableName, Record record) throws IOException {
		
		if  (HLog.l.isInfoEnabled()) 
			HLog.l.info("HWriter: update (" + tableName + ") , PK=" + record.pk.toString());
		
		Put update = new Put(record.pk.toBytes());
   		for (NV packet : record.getNVs()) {
			update.add(packet.family.toBytes(), packet.name.toBytes(), packet.data.toBytes());
		}	    

   		HTableWrapper table = null;
		HBaseFacade facade = null;
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			update.setWriteToWAL(true);
			table.put(update);
			table.flushCommits();
		} finally {
			if ( null != facade && null != table) {
				facade.putTable(table);
			}
		}
	}
	
	
	/**
	 * Delete the complete row based on the key
	 * @param tableName
	 * @param pk
	 * @throws IOException
	 */
	public static void delete(String tableName, IStorable pk) throws IOException {
		Delete delete = new Delete(pk.toBytes());

		HBaseFacade facade = HBaseFacade.getInstance();
		HTableWrapper table = null;
		try {
			table = facade.getTable(tableName);
			table.delete(delete);
			table.flushCommits();
		} finally {
			if ( null != facade && null != table) {
				facade.putTable(table);
			}
		}
	}
	
	/**
	 * Delete the complete row based on the key
	 * @param tableName
	 * @param pk
	 * @throws IOException
	 */
	public static void delete(String tableName, List<byte[]> pks) throws IOException {
		
		if ( null == pks) return;
		if (pks.size() == 0 ) return;
		
		HBaseFacade facade = HBaseFacade.getInstance();
		HTableWrapper table = null;

		RowLock lock = null;
		
		try {
			table = facade.getTable(tableName);
			for (byte[] pk : pks) {
				Delete delete = new Delete(pk);
				table.delete(delete);
			}
			table.flushCommits();
		} finally {
			if ( null != lock) table.unlockRow(lock);
			if ( null != facade && null != table) {
				facade.putTable(table);
			}
		}
	}	
	
	/**
	 * Deletes the row and flushes the coomit.
	 * The packet field values can be null.
	 * @param tableName
	 * @param pk
	 * @param packet
	 * @throws IOException
	 */
	public static void delete(String tableName, IStorable pk, NV packet) throws IOException {
		
		Delete delete = new Delete(pk.toBytes());
		delete = delete.deleteColumns(packet.family.toBytes(), packet.name.toBytes());
		
		HBaseFacade facade = HBaseFacade.getInstance();
		HTableWrapper table = null;
		try {
			table = facade.getTable(tableName);
			table.delete(delete);
			table.flushCommits();
		} finally {
			if ( null != facade && null != table) {
				facade.putTable(table);
			}
		}
	}
	
	
	/**
	 * Before putting the record, it merges the record
	 * It happens without the locking mechanism
	 * @param tableName
	 * @param records
	 * @throws IOException
	 */
	public static void mergeScalar(String tableName, List<RecordScalar> records) 
	throws IOException {
			
		if ( null == tableName  || null == records) return;
		if  (HLog.l.isInfoEnabled()) 
			HLog.l.info("HWriter: mergeScalar (" + tableName + ") , Count =" + records.size());

   		HTableWrapper table = null;
		HBaseFacade facade = null;
		List<RowLock> locks = new ArrayList<RowLock>();
		
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);

			int recordsT = records.size();
			List<Put> updates = ( recordsT > 300 ) ? 
				new Vector<Put>(recordsT) : new ArrayList<Put>(recordsT);
			
			for (RecordScalar scalar : records) {
				byte[] pk = scalar.pk.toBytes();
				Get getter = new Get(pk);
				byte[] famB = scalar.kv.family.toBytes();
				byte[] nameB = scalar.kv.name.toBytes();
				RowLock lock = null;
				if ( table.exists(getter) ) {
					lock = table.lockRow(pk);
					locks.add(lock);
					Get existingGet = new Get(pk, lock);
					existingGet.addColumn(famB, nameB);
					Result r = table.get(existingGet); 
					if ( ! scalar.merge(r.getValue(famB, nameB)) ) continue;
				}

				Put update = ( null == lock ) ? new Put(pk) :  new Put(pk, lock);
				NV kv = scalar.kv;
				update.add(famB,nameB, kv.data.toBytes());
				updates.add(update);
			}
			
			table.put(updates);
			table.flushCommits();

		} finally {
			for (RowLock lock: locks) {
				try { table.unlockRow(lock); } catch (Exception ex) {HLog.l.warn("Ignore Unlock exp :" , ex);}
			}
			if ( null != facade && null != table) {
				facade.putTable(table);
			}
		}
	}
	
	
	/**
	 * Merge a record accessing the existing value
	 * It happens with the locking mechanism
	 * @param tableName
	 * @param records
	 * @throws IOException
	 */
	public static void merge(String tableName, Record record) 
	throws IOException {
			
		if ( null == tableName  || null == record) return;
		if  (HLog.l.isInfoEnabled()) 
			HLog.l.info("HWriter: merge Record (" + tableName + ")") ;

   		HTableWrapper table = null;
		HBaseFacade facade = null;
		RowLock lock = null;
		
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);

			byte[] pk = record.pk.toBytes();
			Get getter = new Get(pk);
			if ( table.exists(getter) ) {
				lock = table.lockRow(pk);
				Get existingGet = new Get(pk, lock);
				Result r = table.get(existingGet);
				if ( null == r) return;
				for (KeyValue kv : r.list()) {
					if ( ! record.merge(kv.getFamily(), 
						kv.getQualifier(), kv.getValue() ) ) continue;
				}
			}

			Put update = ( null == lock ) ? new Put(pk) :  new Put(pk, lock);
			for (NV nv : record.getNVs()) {
				update.add(new KeyValue(pk, nv.family.toBytes(), nv.name.toBytes(), nv.data.toBytes()) );	
			}

			table.put(update);
			table.flushCommits();

		} finally {
			if ( null != lock ) {
				try { table.unlockRow(lock); } catch (Exception ex) {HLog.l.warn("Ignore Unlock exp :" , ex);}
			}
			if ( null != facade && null != table) {
				facade.putTable(table);
			}
		}
	}	
}