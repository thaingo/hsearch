package com.bizosys.hsearch.outpipe;

import java.util.List;

import com.bizosys.hsearch.query.DocMetaWeight;
import com.bizosys.hsearch.query.HQuery;
import com.bizosys.hsearch.query.QueryContext;
import com.bizosys.hsearch.query.QueryResult;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeOut;

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
		
		CheckMetaInfoHBase hbase = new CheckMetaInfoHBase(ctx);
		List<DocMetaWeight> dmwL = hbase.filter(staticL, ctx.scroll, this.pageSize);
		if ( null == dmwL) return true;
		result.sortedDynamicWeights = dmwL.toArray();
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
