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
package com.bizosys.hsearch.dictionary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.services.batch.BatchTask;

import com.bizosys.hsearch.hbase.HBaseFacade;
import com.bizosys.hsearch.hbase.HTableWrapper;
import com.bizosys.hsearch.schema.IOConstants;

/**
 * Loads the dictionry terms to memory in intervals.
 * It helps for fuzzy search and regex search.
 * @author karan
 */
public class DictionaryRefresh implements BatchTask {

	/**
	 * The job name
	 */
	private String jobName = "DictionaryRefresh";
	
	/**
	 * Run in incremental
	 */
	private boolean isIncremental = true;
	
	/**
	 * When the last time the Job ran
	 */
	private long lastProcessingTime = -1;
	
	public String getJobName() {
		return this.jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public Object process() throws ApplicationFault, SystemFault{
		
		/**
		 * download all new changes 
		 */
		
		long now = new Date().getTime();
		
		Scan scan = new Scan();
		scan.setCaching(300);
		scan.setCacheBlocks(false);

		scan = scan.setMaxVersions(1);
		scan = scan.addColumn(
			IOConstants.DICTIONARY_BYTES,IOConstants.DICTIONARY_TERM_BYTES);
		
		try {
			if ( -1 != lastProcessingTime && isIncremental ) {
				scan = scan.setTimeRange(lastProcessingTime, now);
			}
			
			HTableWrapper table = HBaseFacade.getInstance().getTable(IOConstants.TABLE_DICTIONARY);
			ResultScanner iterator = table.getScanner(scan);
			
			List<byte[]> words = new ArrayList<byte[]>();
			for ( Result r : iterator ) {
				if ( null == r) continue;
				if ( r.isEmpty()) continue;
				byte[] term = 
					r.getValue(IOConstants.DICTIONARY_BYTES,IOConstants.DICTIONARY_TERM_BYTES);
				if ( null != term && term.length > 0 ) words.add(term);
			}
			lastProcessingTime = now;
			return true;
		} catch (IOException ex) {
			DictionaryLog.l.error("DictionaryRefresh : Failure", ex);
			throw new SystemFault(ex);
		}
	}
}
