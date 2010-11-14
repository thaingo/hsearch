package com.bizosys.hsearch.outpipe;

import java.util.List;

import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeOut;
import com.bizosys.oneline.util.StringUtils;

import com.bizosys.hsearch.index.DocumentType;
import com.bizosys.hsearch.index.TermType;
import com.bizosys.hsearch.query.HQuery;
import com.bizosys.hsearch.query.QueryContext;
import com.bizosys.hsearch.query.QueryPlanner;
import com.bizosys.hsearch.query.QueryTerm;

public class ComputeTypeCodes implements PipeOut{
	
	public ComputeTypeCodes() {
	}	

	public boolean visit(Object objQuery) throws ApplicationFault, SystemFault {
		HQuery query = (HQuery) objQuery;
		QueryContext ctx = query.ctx;
		QueryPlanner planner = query.planner;
		
		this.deduceDocumentType(ctx);
		this.deduceTermType(planner.mustTerms);
		this.deduceTermType(planner.optionalTerms);
		
		return true;
	}
	
	private void deduceDocumentType(QueryContext ctx ) throws ApplicationFault {
		
		if ( null == ctx) return ;
		if ( null == ctx.docType) return ;
		
		ctx.docTypeCode = DocumentType.getInstance().getTypeCode(ctx.docType);
	}
	
	private void deduceTermType(List<QueryTerm> queryWordL) throws ApplicationFault {
		if ( null == queryWordL) return;
		for (QueryTerm term : queryWordL) {
			if ( StringUtils.isEmpty(term.termType) ) continue;
			term.termTypeCode = 
				TermType.getInstance().getTypeCode(term.termType);
		}
	}
	
	public boolean commit() throws ApplicationFault, SystemFault {
		return true;
	}

	public PipeOut getInstance() {
		return this;
	}

	public boolean init(Configuration conf) throws ApplicationFault, SystemFault {
		return true;
	}
}
