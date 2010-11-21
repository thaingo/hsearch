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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTablePool;

/**
 * HTable.flushCommits() on Shutdown
 * @author bizosys
 *
 */
public class HBaseFacade {

	protected Configuration conf;
	protected HBaseAdmin admin = null;
	protected HTableDescriptor desc = null;
	
	private static HBaseFacade instance = null;
	/**
	 * Give a static instance
	 * @return
	 * @throws IOException
	 */
	public static HBaseFacade getInstance() throws IOException
	{
		if ( null != instance ) return instance;
		synchronized (HBaseFacade.class) {
			if ( null != instance ) return instance;
			instance = new HBaseFacade();
		}
		return instance;
	}

	
	
	private HBaseFacade() throws IOException{
		HLog.l.debug("HBaseFacade > Initializing HBaseFacade");
		conf = HBaseConfiguration.create();
		try {
			admin = new HBaseAdmin(conf);
			HBaseFacade.instance = this;
			HLog.l.debug("HBaseFacade > HBaseFacade initialized.");
		} catch (MasterNotRunningException ex) {
			throw new IOException ("HBaseFacade > HBase Master instance is not running..");			
		}
	}
	
	/**
	 * Don't use the admin.shutdown() - This shuts down the hbase instance.
	 *
	 */
	public void stop() {
		try {
			admin.shutdown();
		} catch (IOException ex) {
			
		}
	}
	
	public HBaseAdmin getAdmin() throws IOException {
		if ( null == admin) throw new IOException ("HBaseFacade > HBase Service is not initialized");
		return admin;
	}
	
    HTablePool pool = null;
    int liveTables = 0; 
	public HTableWrapper getTable(String tableName) throws IOException {
		
		if ( null == pool ) pool = new HTablePool(this.conf, Integer.MAX_VALUE);
		//if ( HLog.l.isDebugEnabled() ) HLog.l.debug("HBaseFacade > Live hbase tables : " + liveTables); 
		
		HTableWrapper table = new HTableWrapper( tableName, pool.getTable(tableName));
		liveTables++;
		return table;
	}

	public void putTable(HTableWrapper table) {
		if ( null == pool ) return;
		pool.putTable(table.table);
		liveTables--;
	}
	
	public void recycleTable(HTableWrapper table) throws IOException {
		if ( null == pool ) return;
		pool.putTable(table.table);
		table.table = pool.getTable(table.tableName);
	}	
	
	public int getLiveTables() {
		return this.liveTables;
	}
}