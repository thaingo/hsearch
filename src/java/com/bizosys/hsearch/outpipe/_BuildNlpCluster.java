package com.bizosys.hsearch.outpipe;

import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeOut;

public class _BuildNlpCluster implements PipeOut{
	
	public _BuildNlpCluster() {
	}	

	public boolean visit(Object objQuery) throws ApplicationFault, SystemFault {
		//HQuery query = (HQuery) objQuery;
		//QueryContext ctx = query.ctx;
		//QueryPlanner planner = query.planner;

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
