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

import java.util.Date;

import com.bizosys.hsearch.index.DocMeta;
import com.bizosys.hsearch.query.DocMetaWeight;
import com.bizosys.hsearch.query.HQuery;
import com.bizosys.hsearch.query.QueryContext;
import com.bizosys.hsearch.query.QueryPlanner;
import com.bizosys.hsearch.query.QueryResult;
import com.bizosys.hsearch.query.QueryTerm;
import com.bizosys.hsearch.util.IpUtil;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeOut;

/**
 * Ranks the document based on term and meta information.
 * Following criterias are taken for finding the dynamic ranking
 * <lu>
 * <li>Freshness</li>
 * <li>IP Proximity</li>
 * <li>Sighting on author Tag words</li>
 * <li>Sighting on User Tag words</li>
 * </lu> 
 * @author karan
 *
 */
public class ComputeDynamicRanking implements PipeOut{
	
	public ComputeDynamicRanking() {
	}	

	public boolean visit(Object objQuery) throws ApplicationFault, SystemFault {
		
		HQuery query = (HQuery) objQuery;
		QueryResult result = query.result;
		QueryContext ctx = query.ctx;
		QueryPlanner plan = query.planner;
		int ipHouse = ( null == ctx.ipAddress ) ? 0 : IpUtil.computeHouse(ctx.ipAddress);
		if ( null == result.sortedDynamicWeights) return true;
		
		for (Object metaO : result.sortedDynamicWeights) {
			DocMetaWeight meta = (DocMetaWeight) metaO;
			meta.weight = meta.weight + this.scoreFreshness(meta);
			if ( 0 != ipHouse ) meta.weight = meta.weight +  
				this.scoreIpProximity(meta, ipHouse);
			meta.weight = meta.weight + this.scoreSocialText(meta, plan);
			meta.weight = meta.weight + this.scoreTags(meta, plan);
		}
		DocMetaWeight.sort(result.sortedDynamicWeights);
		return true;
	}
	
	private int scoreFreshness(DocMeta meta) {
		Date referenceDate = meta.modifiedOn;
		if ( null == referenceDate ) {
			referenceDate = meta.createdOn;
		}
		if ( null == referenceDate ) return 0;

		double totalScore = System.currentTimeMillis() - referenceDate.getTime();
		totalScore = 100 - (totalScore / 1170000000L);
		int score = new Double(totalScore).intValue();
		if ( score < 0 ) score = 0;
		if ( OutpipeLog.l.isDebugEnabled()) OutpipeLog.l.debug("ComputeDynamicRanking:Freshness : " + score );
		return score;
	}
	
	private int scoreIpProximity(DocMeta meta, int ipHouse) {
		int ipScore = meta.ipHouse - ipHouse;
		ipScore = (ipScore < 0) ? ipScore * -1 : ipScore;
		if ( ipScore == 0) {
			ipScore = 100; //Complete Match
		} else {
			ipScore = new Double(100 - (Math.log10(ipScore) / 9 * 100)).intValue();
			ipScore = (ipScore < 0) ? ipScore * -1 : ipScore;
		}
		if ( OutpipeLog.l.isDebugEnabled()) OutpipeLog.l.debug("ComputeDynamicRanking:IpScopre :" + ipScore);
		return ipScore; 
	}
	

	/**
	 * For each social text found in matching to term work
	 * 1 point is contributed for ranking. 
	 * @param meta
	 * @param planner
	 * @return
	 */
	private int scoreSocialText(DocMeta meta, QueryPlanner planner) {
		if (null == meta.socialText) return 0;
		int socialRanking = 0;
		if ( null != planner.mustTerms) {
			for (QueryTerm term : planner.mustTerms) {
				if ( meta.socialText.indexOf(term.wordOrigLower) >= 0 ) {
					socialRanking++;
				}
			}
		}
		
		if ( null != planner.optionalTerms) {
			for (QueryTerm term : planner.mustTerms) {
				if ( meta.socialText.indexOf(term.wordOrigLower) >= 0 ) {
					socialRanking++;
				}
			}
		}
		
		if ( OutpipeLog.l.isDebugEnabled()) OutpipeLog.l.debug("ComputeDynamicRanking:socialRanking : " + socialRanking );
		return socialRanking;
	}
	
	/**
	 * For each term word found in social text which matches the provided tag 
	 * word, 1 point is contributed for ranking. 
	 * @param meta
	 * @param planner
	 * @return
	 */
	private int scoreTags(DocMeta meta, QueryPlanner planner) {
		if (null == meta.tags) return 0;
		int tagRanking = 0;
		if ( null != planner.mustTerms) {
			for (QueryTerm term : planner.mustTerms) {
				if ( meta.tags.indexOf(term.wordOrigLower) >= 0 ) {
					tagRanking++;
				}
			}
		}
		
		if ( null != planner.optionalTerms) {
			for (QueryTerm term : planner.mustTerms) {
				if ( meta.tags.indexOf(term.wordOrigLower) >= 0 ) {
					tagRanking++;
				}
			}
		}
		
		if ( OutpipeLog.l.isDebugEnabled()) OutpipeLog.l.debug("ComputeDynamicRanking:Tags : " + tagRanking );
		return tagRanking;
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
	
	public String getName() {
		return "ComputeDynamicRanking";
	}		
}
