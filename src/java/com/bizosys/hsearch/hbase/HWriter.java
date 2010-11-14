package com.bizosys.hsearch.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.Scan;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;

import com.bizosys.hsearch.common.IStorable;
import com.bizosys.hsearch.util.Record;
import com.bizosys.hsearch.util.RecordScalar;


public class HWriter {

    public final static long FORCE_UPDATE  = -1;
    
    /**
	 * Creates the table if not existing before
	 * @param tableName
	 * @param cols
	 * @throws IOException
	 */
    public static final void create(String tableName, List<HColumnDescriptor> cols) 
    throws SystemFault, ApplicationFault {
    	
		if  (HLog.l.isDebugEnabled()) 
			HLog.l.debug("Creating HBase Table - " + tableName);
		
		try {
			if  (HLog.l.isDebugEnabled()) 
				HLog.l.debug("Checking for table existance : " + tableName);
			HBaseAdmin admin =  HBaseFacade.getInstance().getAdmin();
        	if ( admin.tableExists(tableName)) {

        		if  (HLog.l.isInfoEnabled()) 
	        		HLog.l.info("Ignoring creation. Table already exists - " + tableName);
        	} else {
        		HTableDescriptor tableMeta = new HTableDescriptor(tableName);
        		for (HColumnDescriptor col : cols) tableMeta.addFamily(col);
        		admin.createTable(tableMeta);
        		if  (HLog.l.isInfoEnabled() ) HLog.l.info("Table Created - " + tableName);
        	}

		} catch (TableExistsException ex) {
			HLog.l.warn("Ignoring creation. Table already exists - " + tableName, ex);
			throw new ApplicationFault("Failed Table Creation : " + tableName, ex);
		} catch (MasterNotRunningException mnre) {
			throw new SystemFault("Failed Table Creation : " + tableName, mnre);
		} catch (IOException ioex) {
			throw new SystemFault("Failed Table Creation : " + tableName, ioex);
		}
	}
    
    
	/**
	 * Drop a table. This may take significantly large time as things
	 * are disabled first and then gets deleted. 
	 * @param tableName
	 * @throws IOException
	 */
	public static void drop(String tableName) throws SystemFault, ApplicationFault {

		if  (HLog.l.isDebugEnabled()) 
			HLog.l.debug("Checking for table existance");
		
		try {
			HBaseAdmin admin =  HBaseFacade.getInstance().getAdmin();
	    	byte[] bytesTableName = tableName.getBytes();
			if ( admin.tableExists(bytesTableName)) {
	    		if ( ! admin.isTableDisabled(bytesTableName) ) 
	    			admin.disableTable(bytesTableName);
	    		if ( admin.isTableDisabled(bytesTableName) ) 
	    				admin.deleteTable(bytesTableName);
				if  (HLog.l.isInfoEnabled() ) HLog.l.info (tableName + " Table is deleted.");
	    	} else {
	    		HLog.l.warn( tableName + " table is not found during drop operation.");
	    		throw new ApplicationFault("Table does not exist");
	    	}
		} catch (IOException ioex) {
			throw new SystemFault("Table Drop Failed : " + tableName, ioex);
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
	public static void update(String tableName, Record record, boolean nonBatch) throws IOException {
		
		if  (HLog.l.isInfoEnabled()) 
			HLog.l.info("<o><n>update_table</n><t>" + tableName + "</t><r>" + record.pk.toString() + "</r></o>");
		
		Put update = new Put(record.pk.toBytes());
   		for (NV packet : record.nvs) {
			update.add(packet.family.toBytes(), packet.name.toBytes(), packet.data.toBytes());
		}	    

   		HTableWrapper table = null;
		HBaseFacade facade = null;
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			update.setWriteToWAL(nonBatch);
			table.put(update);
			if ( nonBatch ) table.flushCommits();
		} finally {
			if ( null != facade && null != table) {
				facade.putTable(table);
			}
		}
	}
	
	/**
	 * If FORCE_UPDATE, it does not check the read time of the record.
	 * Check the read time of the record. 
	 *   
	 * @param tableName
	 * @param records
	 * @param checkoutTime
	 * @throws IOException
	 */
	public static void update(String tableName, List<Record> records, boolean nonBatch) throws IOException {
		
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
			update.setWriteToWAL(nonBatch);
	   		updates.add(update);
		}
		HTableWrapper table = null;
		HBaseFacade facade = null;
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			table.put(updates);
			if ( nonBatch ) table.flushCommits();
			
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
	public static void insert(String tableName, Record record, boolean nonBatch ) throws IOException {
		byte[] pk = record.pk.toBytes();
		
   		HTableWrapper table = null;
		HBaseFacade facade = null;
		RowLock lock = null;
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			
			if (nonBatch) {
				lock = table.lockRow(pk);
				Put update = new Put(pk, lock);
		   		for (NV param : record.nvs) {
					update.add(param.family.toBytes(), 
							param.name.toBytes(), param.data.toBytes());
				}	   			
				update.setWriteToWAL(nonBatch);				
				table.put(update);
				table.unlockRow(lock);
				table.flushCommits();
				lock = null;
			} else {
				Put update = new Put(pk);
		   		for (NV param : record.nvs) {
					update.add(param.family.toBytes(), 
							param.name.toBytes(), param.data.toBytes());
				}	   			
				update.setWriteToWAL(nonBatch);				
				table.put(update);
				//HLog.l.debug(tableName + " # " + record.toString());
			}
		} finally {
			if ( null != lock) table.unlockRow(lock);
			if ( null != facade && null != table) {
				facade.putTable(table);
			}
		}
	}
	
	public static void insert(String tableName, RecordScalar record, boolean nonBatch ) throws IOException {
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
			update.setWriteToWAL(nonBatch);
			table.put(update);
			if ( nonBatch ) table.flushCommits();
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
	public static void insert(String tableName, List<Record> records, boolean nonBatch ) throws IOException {
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
	   		update.setWriteToWAL(nonBatch);
			updates.add(update);
		}
		HTableWrapper table = null;
		HBaseFacade facade = null;
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			table.put(updates);
			if ( nonBatch ) table.flushCommits();
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
	public static void insertScalar(String tableName, RecordScalar record, boolean nonBatch ) throws IOException {
		if  (HLog.l.isDebugEnabled()) 
			HLog.l.debug("insertScalar 1 : " + tableName);
		
		byte[] pk = record.pk.toBytes();
		Put update = new Put(pk);
		NV kv = record.kv;
		update.add(kv.family.toBytes(),kv.name.toBytes(), kv.data.toBytes());
   		update.setWriteToWAL(nonBatch);
		
		HTableWrapper table = null;
		HBaseFacade facade = null;
		RowLock lock = null;
		try {
			if (nonBatch) {
				lock = table.lockRow(pk);
				table.put(update);
				table.unlockRow(lock);
				table.flushCommits();
				lock = null;
			} else {
				table.put(update);
			}			
		} finally {
			if ( null != lock) table.unlockRow(lock);
			if ( null != facade && null != table) {
				facade.putTable(table);
			}
		}
	}	
	
	public static void insertScalar(String tableName, List<RecordScalar> records, boolean nonBatch ) throws IOException {
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
	   		update.setWriteToWAL(nonBatch);
			updates.add(update);
		}
		HTableWrapper table = null;
		HBaseFacade facade = null;
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			table.put(updates);
			if ( nonBatch ) table.flushCommits();
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
	public static void delete(String tableName, IStorable pk, boolean nonBatch) throws IOException {
		Delete delete = new Delete(pk.toBytes());

		HBaseFacade facade = HBaseFacade.getInstance();
		HTableWrapper table = null;
		try {
			table = facade.getTable(tableName);
			table.delete(delete);
			if ( nonBatch ) table.flushCommits();
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
	public static void delete(String tableName, List<byte[]> pks, boolean nonBatch) throws IOException {
		
		if ( null == pks) return;
		if (pks.size() == 0 ) return;
		
		HBaseFacade facade = HBaseFacade.getInstance();
		HTableWrapper table = null;

		RowLock lock = null;
		
		try {
			table = facade.getTable(tableName);
			for (byte[] pk : pks) {
				if ( nonBatch ) {
					Delete delete = new Delete(pk);
					table.delete(delete);
				} else {
					Delete delete = new Delete(pk);
					table.delete(delete);
				}
				
			}
			if ( nonBatch ) table.flushCommits();
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
	public static void delete(String tableName, IStorable pk, NV packet, boolean nonBatch) throws IOException {
		
		Delete delete = new Delete(pk.toBytes());
		delete = delete.deleteColumns(packet.family.toBytes(), packet.name.toBytes());
		
		HBaseFacade facade = HBaseFacade.getInstance();
		HTableWrapper table = null;
		try {
			table = facade.getTable(tableName);
			table.delete(delete);
			if ( nonBatch ) table.flushCommits();
		} finally {
			if ( null != facade && null != table) {
				facade.putTable(table);
			}
		}
	}
	
	public static void truncate(String tableName, NV kv) throws IOException {
		
		HBaseFacade facade = null;
		ResultScanner scanner = null;
		HTableWrapper table = null;
		List<byte[]> matched = null;
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			
			Scan scan = new Scan();
			scan.setCacheBlocks(true);
			scan.setCaching(500);
			scan.setMaxVersions(1);
			scan = scan.addColumn(kv.family.toBytes(), kv.name.toBytes());
			scanner = table.getScanner(scan);
			
			for (Result r: scanner) {
				if ( null == r) continue;
				if ( r.isEmpty()) continue;
				Delete delete = new Delete(r.getRow());
				delete = delete.deleteColumns(kv.family.toBytes(), kv.name.toBytes());
				table.delete(delete);
			}
		} finally {
			table.flushCommits();
			if ( null != scanner) scanner.close();
			if ( null != table ) facade.putTable(table);
			if ( null != matched) matched.clear();
		}
	}	
}
