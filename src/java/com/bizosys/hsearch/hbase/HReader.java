package com.bizosys.hsearch.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.oneline.ApplicationFault;

import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.util.RecordScalar;

public class HReader {
	
	/**
	 * Scalar data will contain the amount to increase
	 * @param tableName
	 * @param scalar
	 * @throws ApplicationFault
	 */
	public static long generateKeys(String tableName, RecordScalar scalar, long amount ) throws ApplicationFault{
		HBaseFacade facade = null;
		HTableWrapper table = null;
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			long incrementedValue = table.incrementColumnValue(
					scalar.pk.toBytes(), scalar.kv.family.toBytes(), 
					scalar.kv.name.toBytes(), amount);
			return incrementedValue;
		} catch (Exception ex) {
			throw new ApplicationFault("Error in getScalar :" + scalar.toString(), ex);
		} finally {
			if ( null != facade && null != table) facade.putTable(table);
		}
	}
	
	public static boolean exists (String tableName, byte[] pk) throws ApplicationFault{
		HBaseFacade facade = null;
		HTableWrapper table = null;
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			Get getter = new Get(pk);
			return table.exists(getter);
		} catch (Exception ex) {
			throw new ApplicationFault("Error in existance checking :" + pk.toString(), ex);
		} finally {
			if ( null != facade && null != table) facade.putTable(table);
		}
	}
	
	public static List<NVBytes> getCompleteRow (String tableName, byte[] pk) throws ApplicationFault{
		HBaseFacade facade = null;
		HTableWrapper table = null;
		Result r = null;
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			Get getter = new Get(pk);
			if ( table.exists(getter) ) {
				r = table.get(getter);
				List<NVBytes> nvs = new ArrayList<NVBytes>(r.list().size());
				for (KeyValue kv : r.list()) {
					nvs.add( new NVBytes(kv.getFamily(),kv.getQualifier(), kv.getValue()));
				}
				return nvs;
			}
			return null;
		} catch (Exception ex) {
			throw new ApplicationFault("Error in existance checking :" + pk.toString(), ex);
		} finally {
			if ( null != facade && null != table) facade.putTable(table);
		}
	}	
	
	public static void getScalar (String tableName, RecordScalar scalar) throws ApplicationFault{
		HBaseFacade facade = null;
		HTableWrapper table = null;
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			Get getter = new Get(scalar.pk.toBytes());
			Result result = table.get(getter);
			byte[] val = result.getValue(scalar.kv.family.toBytes(), scalar.kv.name.toBytes());
			if ( null != val ) scalar.kv.data = new Storable(val); 
		} catch (Exception ex) {
			throw new ApplicationFault("Error in getScalar :" + scalar.toString(), ex);
		} finally {
			if ( null != facade && null != table) facade.putTable(table);
		}
	}
	
	public static void getAllValues(String tableName, NV kv, IScanCallBack callback ) 
		throws ApplicationFault {
		
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
			
			long timeS = System.currentTimeMillis();
			for (Result r: scanner) {
				if ( null == r) continue;
				if ( r.isEmpty()) continue;
				
				byte[] storedBytes = r.getValue(kv.family.toBytes(), kv.name.toBytes());
				if ( null == storedBytes) continue;
				callback.process(storedBytes);
			}
			if ( HLog.l.isDebugEnabled()) {
				long timeE = System.currentTimeMillis();
				HLog.l.debug("HReader.getAllValues (" + tableName + ") = " + 
					(timeE - timeS) );
			}
			
		} catch ( IOException ex) {
			throw new ApplicationFault(ex);
		} finally {
			if ( null != scanner) scanner.close();
			if ( null != table ) facade.putTable(table);
			if ( null != matched) matched.clear();
		}
	}
	
	/**
	 * Get the keys of the table
	 * @param tableName
	 * @param kv
	 * @param startKey
	 * @param pageSize
	 * @return
	 * @throws ApplicationFault
	 */
	public static List<byte[]> getAllKeys(String tableName, NV kv, 
		byte[] startKey, int pageSize) throws ApplicationFault {
	
		HBaseFacade facade = null;
		ResultScanner scanner = null;
		HTableWrapper table = null;
		List<byte[]> keys = new ArrayList<byte[]>(pageSize);
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
		
			Scan scan = new Scan();
			scan.setCacheBlocks(true);
			scan.setCaching(500);
			scan.setMaxVersions(1);
			scan = scan.addColumn(kv.family.toBytes(), kv.name.toBytes());
			if ( null != startKey) scan = scan.setStartRow(startKey); 
			scanner = table.getScanner(scan);
			
			int counter = 0;
			for (Result r: scanner) {
				if ( null == r) continue;
				if ( r.isEmpty()) continue;
				
				if ( counter++ > pageSize) break;
				keys.add(r.getRow());
			}
			return keys;
		} catch ( IOException ex) {
			throw new ApplicationFault(ex);
		} finally {
			if ( null != scanner) scanner.close();
			if ( null != table ) facade.putTable(table);
		}
	}
}
