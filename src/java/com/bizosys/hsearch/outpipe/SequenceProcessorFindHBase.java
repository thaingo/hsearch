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
package com.bizosys.hsearch.outpipe;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.Callable;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.filter.TermFilter;
import com.bizosys.hsearch.hbase.HBaseFacade;
import com.bizosys.hsearch.hbase.HTableWrapper;
import com.bizosys.hsearch.index.DocumentType;
import com.bizosys.hsearch.index.TermList;
import com.bizosys.hsearch.index.TermType;
import com.bizosys.hsearch.query.QueryLog;
import com.bizosys.hsearch.query.QueryTerm;

/**
 * This implements callable interface for execution in parallel
 * This actually executes and fetches IDs from the HBase table.
 * @author karan
 *
 */
class SequenceProcessorFindHBase implements Callable<Object> {
	
	public TermFilter tf;
	public List<byte[]> foundBuckets = new Vector<byte[]>();
	
	private boolean isBlockCache = true;
	private int scanIpcLimit = 300;
	private Map<Long, TermList> lastTermLists = null;
	private long fromTime = -1;
	private long toTime = System.currentTimeMillis();
	private QueryTerm term = null;
	
	public SequenceProcessorFindHBase(QueryTerm term, List<byte[]> findWithinBuckets) {
		
		this.term = term;
		
		int totalBytes = 6 /** Hashcode + DocType + TermType*/;
		if ( null != findWithinBuckets) {
			totalBytes = totalBytes + findWithinBuckets.size() * 8;
		}
		
		int pos = 0;

		byte[] filterBytes = new byte[totalBytes];
		byte[] hashBytes = Storable.putInt(term.wordStemmed.hashCode());
		System.arraycopy(hashBytes, 0, filterBytes, pos, 4);
		pos = pos + 4;
		filterBytes[pos++] = term.docTypeCode;
		filterBytes[pos++] = term.termTypeCode;
		
		if ( null != findWithinBuckets) {
			for (byte[] bucket: findWithinBuckets) {
				System.arraycopy(bucket, 0, filterBytes, pos, 8);
				pos = pos + 8;
			}
		}
		this.tf = new TermFilter(filterBytes);
	}
	
	/**
	 * This filters based on the last term ids
	 * The subset is only kept.
	 * Non matching buckets are removed and Document positions marked -1
	 * @param lastMustTerm
	 */
	public void setFilterByIds(QueryTerm lastMustTerm) {
		if ( null == lastMustTerm.foundIds) return;
		if ( 0 == lastMustTerm.foundIds.size()) return;
		
		this.lastTermLists = lastMustTerm.foundIds;
	}

	/**
	 * Go to respective table, colFamily, call
	 * Pass the Matching IDs, Term Type, Document Type, Security Information
	 * Collect only matching Document Sequences 
	 */
	public Object call() throws Exception {
		
		QueryLog.l.debug("SequenceProcessorFindHBase > Call START");
		if ( null == this.term) return null;
		
		/**
		 * Step 1 Identify table, family and column
		 */
		char tableName = this.term.lang.getTableName(this.term.wordStemmed);
		char familyName = this.term.lang.getColumnFamily(this.term.wordStemmed);
		char colName = this.term.lang.getColumn(this.term.wordStemmed);
		if ( QueryLog.l.isDebugEnabled()) {
			StringBuilder sb = new StringBuilder();
			sb.append("SequenceProcessorFindHBase > Term:").append(this.term.wordOrig);
			sb.append(" , Table [").append(tableName);
			sb.append("] , Family [").append(familyName);
			sb.append("] , Column [").append(colName).append(']');
			QueryLog.l.debug(sb.toString());
		}
		
		/**
		 * Step 2 Configure Filtering mechanism 
		 */
		HTableWrapper table = null;
		HBaseFacade facade = null;
		ResultScanner scanner = null;

		try {
			byte[] familyB = new byte[]{(byte)familyName};
			byte[] nameB = new byte[]{(byte)colName};

			facade = HBaseFacade.getInstance();
			table = facade.getTable(new String(new char[]{tableName}));
			
			/**
			 * Configure the scanning mechanism.
			 */
			Scan scan = new Scan();
			scan.setCacheBlocks(isBlockCache);
			scan.setCaching(scanIpcLimit);
			scan = scan.addColumn(familyB, nameB);
			scan.setMaxVersions(1);
			if ( -1 != fromTime) scan = scan.setTimeRange(fromTime, toTime);

			/**
			 * Configure the remote filtering mechanism.
			 */
			scan = scan.setFilter(this.tf);
			scanner = table.getScanner(scan);
			
			byte[] storedB = null;
			byte[] row = null;
			long rowId = -1L;
			TermList lastTermL = null;
			boolean hasElementsLeft = false;
			boolean hasTypeFilter = ( DocumentType.NONE_TYPECODE != term.docTypeCode 
				|| TermType.NONE_TYPECODE != term.termTypeCode);
			Set<Integer> ignorePos = (hasTypeFilter) ? new HashSet<Integer>() :null;
			
			for (Result r: scanner) {
				if ( null == r) continue;
				if ( r.isEmpty()) continue;
				storedB = r.getValue(familyB, nameB);
				if ( null == storedB) continue;
				
				row = r.getRow();
				rowId = Storable.getLong(0, row);
				
				TermList foundTermL = new TermList();
				if ( hasTypeFilter ) {
					ignorePos.clear();
					foundTermL.loadTerms(storedB,ignorePos,term.docTypeCode, term.termTypeCode);
				} else {
					foundTermL.loadTerms(storedB);
				}
				
				if ( !(null == this.lastTermLists || this.term.isOptional) ) { 
					lastTermL = this.lastTermLists.get(rowId);
					if ( null != lastTermL) {
						hasElementsLeft = foundTermL.intersect(lastTermL);
						if ( ! hasElementsLeft ) continue;
					}
				}
				
				/**
				 * There are definite ID subsets in this bucket.
				 */ 
				this.foundBuckets.add(row);
				this.term.foundIds.put(rowId, foundTermL);
			}
			
		} catch ( IOException ex) {
			QueryLog.l.fatal("SequenceProcessorFindHBase:", ex);
			return null;
		} finally {
			if ( null != scanner) scanner.close();
			if ( null != table ) facade.putTable(table);
		}		
		return null;
	}
}