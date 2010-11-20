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
import com.bizosys.hsearch.util.Record;
import com.bizosys.hsearch.util.RecordScalar;


public class HWriter {

	public static void update(String tableName, 
		byte[] pk, IUpdatePipe pipe) throws IOException {
		
		if ( null == tableName  || null == pk) return;
		if  (HLog.l.isInfoEnabled()) 
			HLog.l.info("HWriter: update (" + tableName + ") , PK=" + pk.toString());

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
				KeyValue updatedKV = new KeyValue(kv.getFamily(), kv.getQualifier(),modifiedB);  
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
	 * Check the update time of the record. 
	 * If checkoutTime is before update time = stale record
	 * If not, Allow update.
	 * 
	 * @param table
	 * @throws IOException
	 */
	public static void update(String tableName, Record record) throws IOException {
		
		if  (HLog.l.isInfoEnabled()) 
			HLog.l.info("HWriter: update (" + tableName + ") , PK=" + record.pk.toString());
		
		Put update = new Put(record.pk.toBytes());
   		for (NV packet : record.nvs) {
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
	
	public static void update(String tableName, List<Record> records) throws IOException {
		
		if  (HLog.l.isDebugEnabled()) 
			HLog.l.debug("Updating the table " + tableName);
		
		List<Put> updates = null;
		int recordsT = records.size();
		if ( recordsT > 300 ) updates = new Vector<Put>(recordsT); 
		else updates = new ArrayList<Put>(recordsT);
		
		for (Record record : records) {
	   		Put update = new Put(record.pk.toBytes());
	   		for (NV packet : record.nvs) {
				update.add(packet.family.toBytes(), packet.name.toBytes(), packet.data.toBytes());
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
	   		for (NV param : record.nvs) {
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
	   		for (NV param : record.nvs) {
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
	

}
