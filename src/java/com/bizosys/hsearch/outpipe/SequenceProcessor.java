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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeOut;
import com.bizosys.oneline.services.async.AsyncProcessor;

import com.bizosys.hsearch.index.TermList;
import com.bizosys.hsearch.query.HQuery;
import com.bizosys.hsearch.query.QueryContext;
import com.bizosys.hsearch.query.QueryPlanner;
import com.bizosys.hsearch.query.QueryTerm;

/**
 * Process each keyword of the given query in multiple steps
 * Must terms gets processes sequentially with search with in IDs
 * Multiple Optional terms gets processes parallely.
 * 
 * All non existing IDs are marked as -1.
 * @author karan
 *
 */
public class SequenceProcessor implements PipeOut{
	
	public SequenceProcessor() {
	}	

	public boolean visit(Object objQuery) throws ApplicationFault, SystemFault {
		HQuery query = (HQuery) objQuery;
		QueryContext ctx = query.ctx;
		QueryPlanner planner = query.planner;
		if ( null == planner.sequences) return false;
		
		try {
			List<byte[]> findWithinBuckets = null;
			QueryTerm lastMustQuery = null;
			
			for (List<QueryTerm> step : planner.sequences) {
				if ( 0 == step.size()) continue;
				if ( 1 == step.size()) { //Intermediatelt only 1 Term (Inline call)
					QueryTerm curQuery = step.get(0);
					if ( null != ctx.docTypeCode ) curQuery.docTypeCode = ctx.docTypeCode;
					SequenceProcessorFindHBase bucketId = new SequenceProcessorFindHBase(curQuery,findWithinBuckets);
					if ( null != lastMustQuery ) bucketId.setFilterByIds(lastMustQuery);
					bucketId.call();

					if ( ! curQuery.isOptional ) {
						findWithinBuckets = bucketId.foundBuckets;
						lastMustQuery = curQuery;
					}
				
				} else { //Lastly multiple Optional terms Process parallely
					List<SequenceProcessorFindHBase> findIdJobs = new ArrayList<SequenceProcessorFindHBase>(step.size()); 
					for(QueryTerm term : step) {
						SequenceProcessorFindHBase bucketId = new SequenceProcessorFindHBase(term,findWithinBuckets);
						if ( null != lastMustQuery ) bucketId.setFilterByIds(lastMustQuery);
						findIdJobs.add(bucketId);
					}
					AsyncProcessor.getInstance().getThreadPool().invokeAll(findIdJobs);
				}
			}
			
			intersectMustQs(planner, lastMustQuery);
			subsetOptQs(planner, lastMustQuery);
			
		} catch (InterruptedException ex) {
			String msg = ( null == planner) ? "Empty Planner" : planner.toString(); 
			OutpipeLog.l.fatal("Interrupted @ SequenceProcessor > " + msg, ex);
			return false;
		} catch (Exception ex) {
			String msg = ( null == planner) ? "Empty Planner" : planner.toString(); 
			OutpipeLog.l.fatal("Failed @ SequenceProcessor > " + msg, ex);
			return false;
		}
		
		return true;
	}

	/**
	 * This subsets across all MUST queries.
	 * Last 2 must queries are already in sync from the processing.
	 * @param planner
	 * @param lastMustQuery
	 */
	private void intersectMustQs(QueryPlanner planner, QueryTerm lastMustQuery) {
		if ( null == lastMustQuery) return;
		int stepsT = planner.sequences.size();
		
		for ( int step = stepsT - 1; step > -1; step--) {

			/**
			 * More than 1 means optional
			 */
			List<QueryTerm> curStep = planner.sequences.get(step);
			if ( curStep.size() != 1) continue; 
			
			/**
			 * Look for must only
			 */
			QueryTerm curQuery = curStep.get(0);
			if ( curQuery.isOptional) continue;
			
			/**
			 * Last must query - Already processed
			 */
			if ( lastMustQuery == curQuery) continue;

			/**
			 * Remove the buckets which are absent and then IDs
			 */
			Map<Long, TermList> curBuckets = curQuery.foundIds;
			Map<Long, TermList> lastBuckets = lastMustQuery.foundIds;
			int curBucketsT = curBuckets.size();
			Iterator<Long> curBucketsItr = curBuckets.keySet().iterator();
			
			for ( int i=0; i<curBucketsT; i++ ) {
				Long bucketId = curBucketsItr.next();
				boolean hasElements = lastBuckets.containsKey(bucketId);
				if ( hasElements) {
					hasElements = curBuckets.get(bucketId).
						intersect(lastBuckets.get(bucketId));
					if ( ! hasElements) {
						curBucketsItr.remove();
						lastBuckets.remove(bucketId);
					}
				} else {
					curBucketsItr.remove();
				}
			}
		}
	}
	
	/**
	 * This subsets across all MUST queries.
	 * Last 2 must queries are already in sync from the processing.
	 * @param planner
	 * @param lastMustQuery
	 */
	private void subsetOptQs(QueryPlanner planner, QueryTerm lastMustQuery) {
		if ( null == lastMustQuery) return;
		int stepsT = planner.sequences.size();
		for ( int step = stepsT -1; step > -1; step--) {
			List<QueryTerm> curStep = planner.sequences.get(step);
			for (QueryTerm curQuery : curStep) {
				if ( !curQuery.isOptional) continue;
				
				/**
				 * Remove the buckets which are absent and then IDs
				 */
				Map<Long, TermList> curBuckets = curQuery.foundIds;
				Map<Long, TermList> lastBuckets = lastMustQuery.foundIds;
				int curBucketsT = curBuckets.size();
				Iterator<Long> curBucketsItr = curBuckets.keySet().iterator();
				for ( int i=0; i<curBucketsT; i++ ) {
					Long bucketId = curBucketsItr.next();
					boolean hasElements = lastBuckets.containsKey(bucketId);
					if ( hasElements) {
						hasElements = curBuckets.get(bucketId).subset(lastBuckets.get(bucketId));
						if ( !hasElements) curBucketsItr.remove();
					} else {
						curBucketsItr.remove();
					}
				}
			}
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
	
	public String getName() {
		return "SequenceProcessor";
	}		
}