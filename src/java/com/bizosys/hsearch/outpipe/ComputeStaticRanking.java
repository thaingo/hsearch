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

import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeOut;

import com.bizosys.hsearch.index.TermList;
import com.bizosys.hsearch.query.DocWeight;
import com.bizosys.hsearch.query.HQuery;
import com.bizosys.hsearch.query.QueryContext;
import com.bizosys.hsearch.query.QueryPlanner;
import com.bizosys.hsearch.query.QueryResult;
import com.bizosys.hsearch.query.QueryTerm;

public class ComputeStaticRanking implements PipeOut{
	
	public ComputeStaticRanking() {
	}	

	public boolean visit(Object objQuery) throws ApplicationFault, SystemFault {
		
		L.l.debug("ComputeStaticRank ENTER");
		HQuery query = (HQuery) objQuery;
		QueryContext ctx = query.ctx;
		QueryPlanner planner = query.planner;
		QueryResult result = query.result;
		
		Map<String, DocWeight> sortedStaticMap = computeWeight(ctx, planner);
		if ( L.l.isDebugEnabled()) {
			if ( null == sortedStaticMap) L.l.debug("ComputeStaticRank NONE");
			else L.l.debug("ComputeStaticRank TOTAL = " + sortedStaticMap.size());
		}
		
		result.sortedStaticWeights = sortedStaticMap.values().toArray();
		DocWeight.sort(result.sortedStaticWeights);
		sortedStaticMap.clear();
		sortedStaticMap = null;
		return true;
	}

	private Map<String, DocWeight> computeWeight(QueryContext ctx, QueryPlanner planner) {
		
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
		
		Map<String, DocWeight> docWeightMap = new Hashtable<String, DocWeight>(250);

		for ( int stepsIndex=0; stepsIndex<stepsT; stepsIndex++) {
			qts = stepsItr.next();
			stepsItr.remove();
			if ( null == qts) continue;
			
			qtSize = qts.size();
			qtItr = qts.iterator();
			for ( int qtIndex=0; qtIndex<qtSize; qtIndex++) {
				QueryTerm qt = qtItr.next(); qtItr.remove(); 
				if ( null == qt) continue;
				
				Map<Long, TermList> founded = qt.foundIds;
				if ( null == founded) continue;
				
				bucketItr = founded.keySet().iterator();
				termSize = founded.size();
				
				for (int termIndex=0; termIndex < termSize; termIndex++ ) {
					bucketId = bucketItr.next();
					tl = founded.get(bucketId);
					if ( null == tl) continue;

					bytePos = -1;
					for ( short docPos : tl.docPos ) {
						bytePos++;
						if ( -1 == docPos) continue;
						sb.delete(0, 100);
						sb.append(bucketId).append('_').append(docPos);
						mappedDocId = sb.toString();
						thisWt = (tl.termWeight[bytePos] * qt.preciousNess) + 1;
						if ( docWeightMap.containsKey(mappedDocId) ) {
							docWeightMap.get(mappedDocId).add(thisWt); 
						} else {
							docWeightMap.put(mappedDocId, new DocWeight(mappedDocId, thisWt) ); 								
						}
					}
					tl.cleanup();
					bucketItr.remove();
				}
				founded.clear();
			}
		}
		planner.sequences.clear();
		return docWeightMap;
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
	
	public String getName() {
		return "ComputeStaticRanking";
	}		
}
