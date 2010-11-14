package com.bizosys.hsearch.outpipe;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.Callable;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.filter.TermFilter;
import com.bizosys.hsearch.hbase.HBaseFacade;
import com.bizosys.hsearch.hbase.HTableWrapper;
import com.bizosys.hsearch.index.TermList;
import com.bizosys.hsearch.query.L;
import com.bizosys.hsearch.query.QueryTerm;

/**
 * This implements callable interface for execution in parallel
 * This actually executes and fetches IDs from the HBase table.
 * @author karan
 *
 */
public class SequenceProcessor_FindHBase implements Callable<Object> {
	
	public TermFilter tf;
	public List<byte[]> foundBuckets = new Vector<byte[]>();
	
	private boolean isBlockCache = true;
	private int scanIpcLimit = 300;
	private Map<Long, TermList> lastTermLists = null;
	private long fromTime = -1;
	private long toTime = System.currentTimeMillis();
	private QueryTerm term = null;
	
	public SequenceProcessor_FindHBase(QueryTerm term, List<byte[]> findWithinBuckets) {
		
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
		
		L.l.debug("BucketDocIdFinder > Call START");
		if ( null == this.term) return null;
		
		/**
		 * Step 1 Identify table, family and column
		 */
		char tableName = this.term.lang.getTableName(this.term.wordStemmed);
		char familyName = this.term.lang.getColumnFamily(this.term.wordStemmed);
		char colName = this.term.lang.getColumn(this.term.wordStemmed);
		if ( L.l.isDebugEnabled()) {
			StringBuilder sb = new StringBuilder();
			sb.append("BucketDocIdFinder > Term:").append(this.term.wordOrig);
			sb.append(" , Table").append(tableName);
			sb.append(" , Family").append(familyName);
			sb.append(" , Column").append(colName);
			L.l.debug(sb.toString());
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
			
			for (Result r: scanner) {
				if ( null == r) continue;
				if ( r.isEmpty()) continue;
				storedB = r.getValue(familyB, nameB);
				if ( null == storedB) continue;
				
				row = r.getRow();
				rowId = Storable.getLong(0, row);
				
				TermList foundTermL = new TermList();
				foundTermL.loadTerms(storedB);
				
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
			L.l.fatal("BucketIdFinder:", ex);
			return null;
		} finally {
			if ( null != scanner) scanner.close();
			if ( null != table ) facade.putTable(table);
		}		
		return null;
	}
}