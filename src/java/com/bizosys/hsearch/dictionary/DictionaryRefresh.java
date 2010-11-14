package com.bizosys.hsearch.dictionary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.oneline.ApplicationFault;
import org.apache.oneline.SystemFault;
import org.apache.oneline.services.batch.BatchTask;

import com.bizosys.hsearch.hbase.HBaseFacade;
import com.bizosys.hsearch.hbase.HTableWrapper;
import com.bizosys.hsearch.schema.IOConstants;

	public class DictionaryRefresh implements BatchTask {

		public String jobName = "RefreshDictionary";
		public boolean isIncremental = true;
		public long lastProcessingTime = -1;
		
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
				HLog.l.error("Dictionary Service Failure", ex);
				throw new ApplicationFault(ex);
			}
		}
}
