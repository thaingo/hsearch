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

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.Scan;

public class HTableWrapper {
	
	HTableInterface table = null;
	String tableName = null;
	
	public HTableWrapper(String tableName, HTableInterface table) {
		this.table = table;
		this.tableName = tableName;
	}

	public byte[] getTableName() {
		return table.getTableName();
	}

	public HTableDescriptor getTableDescriptor() throws IOException {
		return table.getTableDescriptor();
	}

	public boolean exists(Get get) throws IOException {
		return table.exists(get);
	}

	public Result get(Get get) throws IOException{
		return table.get(get);
	}

	public ResultScanner getScanner(Scan scan) throws IOException {
		return table.getScanner(scan);
	}

	public ResultScanner getScanner(byte[] family) throws IOException {
		return table.getScanner(family);
	}

	public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
		return table.getScanner(family, qualifier);
	}

	public void put(Put put) throws IOException {
		try {
			table.put(put);
		} catch ( RetriesExhaustedException ex) {
			HBaseFacade.getInstance().recycleTable(this);
			table.put(put);
		}
	}

	public void put(List<Put> puts) throws IOException {
		try {
			table.put(puts);
		} catch ( RetriesExhaustedException ex) {
			HBaseFacade.getInstance().recycleTable(this);
			table.put(puts);
		}
	}

	public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
		byte[] value, Put put) throws IOException {
		
		return table.checkAndPut(row, family, qualifier,value, put );
	}

	public void delete(Delete delete) throws IOException {
		table.delete(delete );
	}

	public void flushCommits() throws IOException {
		table.flushCommits();
	}

	public void close() throws IOException {
		table.close();
	}

	public RowLock lockRow(byte[] row) throws IOException {
		return table.lockRow(row);
	}

	public void unlockRow(RowLock rl) throws IOException {
		if ( null == rl) return; 
		table.unlockRow(rl);
	}
	
	public long incrementColumnValue(byte[] row,
            byte[] family, byte[] qualifier, long amount) throws IOException {
		
		return table.incrementColumnValue(row, family, qualifier, amount, true);
	}
	
	//public List<Result[]> aggregtate(final Aggregate aggregate) throws IOException;
	  
}