package com.bizosys.hsearch.outpipe;

import java.util.ArrayList;
import java.util.List;

import com.bizosys.hsearch.hbase.HReader;
import com.bizosys.hsearch.hbase.NVBytes;
import com.bizosys.hsearch.index.DocMeta;
import com.bizosys.hsearch.index.DocTeaser;
import com.bizosys.hsearch.query.DocMetaWeight;
import com.bizosys.hsearch.query.DocTeaserWeight;
import com.bizosys.hsearch.query.HQuery;
import com.bizosys.hsearch.query.QueryContext;
import com.bizosys.hsearch.query.QueryPlanner;
import com.bizosys.hsearch.query.QueryResult;
import com.bizosys.hsearch.query.QueryTerm;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeOut;

public class BuildTeaser implements PipeOut{
	
	public BuildTeaser() {
	}	

	public boolean visit(Object objQuery) throws ApplicationFault, SystemFault {
		HQuery query = (HQuery) objQuery;
		QueryContext ctx = query.ctx;
		QueryResult res = query.result;
		if ( null == res) return true;
		if ( null == res.sortedDynamicWeights) return true;
		
		int foundT = res.sortedDynamicWeights.length;
		int maxFetching = ( ctx.documentFetchLimit <  foundT) ? 
				ctx.documentFetchLimit : foundT;
		
		List<DocTeaserWeight> weightedTeasers = new ArrayList<DocTeaserWeight>(maxFetching); 
		for ( int i=0; i< maxFetching; i++) {
			DocMetaWeight metaWt =  (DocMetaWeight) res.sortedDynamicWeights[i];
			byte[] idB = metaWt.id.getBytes();
			List<NVBytes> flds = 
				HReader.getCompleteRow(IOConstants.TABLE_PREVIEW, idB);
			weightedTeasers.add(new DocTeaserWeight(idB, flds,metaWt.weight));
		}
		res.teasers = weightedTeasers.toArray();
		DocTeaserWeight.sort(res.teasers);
		return true;
	}
	
	/**
	 * Here we look for exact match on a very limited set to build the teaser.
	 * @param planner
	 * @param dtw
	 */
	private void scoreExactMatch(QueryPlanner planner, DocTeaserWeight dtw) {
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
