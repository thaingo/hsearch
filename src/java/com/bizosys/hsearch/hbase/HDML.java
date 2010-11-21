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
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;

public class HDML {
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
