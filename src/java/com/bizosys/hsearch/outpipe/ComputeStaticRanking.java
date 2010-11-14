package com.bizosys.hsearch.outpipe;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.oneline.ApplicationFault;
import org.apache.oneline.SystemFault;
import org.apache.oneline.conf.Configuration;
import org.apache.oneline.pipes.PipeOut;

import com.bizosys.hsearch.index.TermList;
import com.bizosys.hsearch.query.HQuery;
import com.bizosys.hsearch.query.QueryContext;
import com.bizosys.hsearch.query.QueryPlanner;
import com.bizosys.hsearch.query.QueryTerm;

public class ComputeStaticRanking implements PipeOut{
	
	public ComputeStaticRanking() {
	}	

	public boolean visit(Object objQuery) throws ApplicationFault, SystemFault {
		
		HQuery query = (HQuery) objQuery;
		QueryContext ctx = query.ctx;
		QueryPlanner planner = query.planner;
		
		Iterator<List<QueryTerm>> stepsItr = planner.sequences.iterator();
		int stepsT = planner.sequences.size();
		StringBuilder sb = new StringBuilder(100);
		
		long bucketId = -1;
		int termSize = -1;
		Iterator<Long> bucketItr = null;
		TermList tl = null;
		int bytePos = -1;
		float thisWt = -1;
		List<QueryTerm> qts = null;
		int qtSize = -1;
		Iterator<QueryTerm> qtItr = null;
		String mappedDocId = null;
		
		for ( int stepsIndex=0; stepsIndex<stepsT; stepsIndex++) {
			
			qts = stepsItr.next();
			stepsItr.remove();
			if ( null == qts) continue;
			
			qtSize = qts.size();
			qtItr = qts.iterator();
			for ( int qtIndex=0; qtIndex<qtSize; qtIndex++) {
				QueryTerm qt = qtItr.next();
				qtItr.remove(); 
				if ( null == qt) continue;
				
				Map<Long, TermList> founded = qt.foundIds;
				if ( null == founded) continue;
				bucketItr = founded.keySet().iterator();
				termSize = founded.size();
				for (int termIndex=0; termIndex < termSize; termIndex++ ) {
					bucketId = bucketItr.next();
					tl = founded.get(bucketId);
					if ( null != tl) {
						bytePos = -1;
 						for ( short docPos : tl.docPos ) {
 							bytePos++;
 							if ( -1 == docPos) continue;
 							sb.delete(0, 100);
 							sb.append(bucketId).append('_').append(docPos);
 							mappedDocId = sb.toString();
 							thisWt = (tl.termWeight[bytePos] * qt.preciousNess) + 1;
 							if ( ctx.docweight.containsKey(mappedDocId) ) {
 								ctx.docweight.put(mappedDocId, ctx.docweight.get(mappedDocId) + thisWt);
 							} else {
 								ctx.docweight.put(mappedDocId, thisWt); 								
 							}
 						}
 						tl.cleanup();
 						bucketItr.remove();
 					}
				}
			}
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
		return true;
	}
}
