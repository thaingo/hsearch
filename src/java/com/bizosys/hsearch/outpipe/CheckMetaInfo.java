package com.bizosys.hsearch.outpipe;

import java.util.List;

import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeOut;

import com.bizosys.hsearch.hbase.HReader;
import com.bizosys.hsearch.hbase.NVBytes;
import com.bizosys.hsearch.query.DocWeight;
import com.bizosys.hsearch.query.HQuery;
import com.bizosys.hsearch.query.QueryContext;
import com.bizosys.hsearch.query.QueryPlanner;
import com.bizosys.hsearch.query.QueryResult;
import com.bizosys.hsearch.schema.IOConstants;

public class CheckMetaInfo implements PipeOut{
	
	int pageSize = 100; //10 Pages each 10 records
	
	public CheckMetaInfo() {
	}	

	public boolean visit(Object objQuery) throws ApplicationFault, SystemFault {
		HQuery query = (HQuery) objQuery;
		QueryResult result = query.result;
		QueryContext ctx = query.ctx;
		if ( null == result) return true;
		
		Object[] staticL = result.sortedStaticWeights;
		if ( null == staticL) return true;
		
		/**
		 * Bring the pointer to beginning from the end
		 */
		int staticT = staticL.length;
		if ( staticT <= ctx.scroll) ctx.scroll = 0;
		
		int totalMatched = 0;
		
		for (int i=ctx.scroll; i< staticT; i++ ) {
			if ( totalMatched > pageSize) break; //Just read enough for the page size 
			
			String id = ((DocWeight) staticL[i]).id;
			byte [] meta = HReader.getScalar(IOConstants.TABLE_PREVIEW, 
				IOConstants.SEARCH_BYTES, IOConstants.META_BYTES, id.getBytes());
			
		}

		if ( null == values) continue;
		for (NVBytes bytes : values) {
			sb.append('\n').append(bytes.toString());
		}
		
		return true;
	}
	
	public boolean commit() throws ApplicationFault, SystemFault {
		return true;
	}

	public PipeOut getInstance() {
		return this;
	}

	public boolean init(Configuration conf) throws ApplicationFault, SystemFault {
		return false;
	}
}
