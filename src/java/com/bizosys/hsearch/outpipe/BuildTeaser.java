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
import java.util.List;

import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.filter.TeaserFilter;
import com.bizosys.hsearch.hbase.HReader;
import com.bizosys.hsearch.hbase.NVBytes;
import com.bizosys.hsearch.query.DocMetaWeight;
import com.bizosys.hsearch.query.DocTeaserWeight;
import com.bizosys.hsearch.query.HQuery;
import com.bizosys.hsearch.query.QueryContext;
import com.bizosys.hsearch.query.QueryResult;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeOut;

public class BuildTeaser implements PipeOut{
	
	int DEFAULT_PAGE_SIZE = 10;
	int DEFAULT_TEASER_LENGTH = 20;
	public BuildTeaser() {
	}	

	public boolean visit(Object objQuery) throws ApplicationFault, SystemFault {
		HQuery query = (HQuery) objQuery;
		QueryContext ctx = query.ctx;
		QueryResult res = query.result;
		if ( null == res) return true;
		if ( null == res.sortedDynamicWeights) return true;
		
		int documentFetchLimit = (-1 == ctx.documentFetchLimit) ? 
			DEFAULT_PAGE_SIZE : ctx.documentFetchLimit;
		System.out.println("KK" + documentFetchLimit + ":" +ctx.documentFetchLimit );
		
		int teaserCutSection = (-1 == ctx.teaserSectionLen) ?
			DEFAULT_TEASER_LENGTH : ctx.teaserSectionLen;
		
		int foundT = res.sortedDynamicWeights.length;
		int maxFetching = ( documentFetchLimit <  foundT) ? 
				documentFetchLimit : foundT;
		
		List<DocTeaserWeight> weightedTeasers = new ArrayList<DocTeaserWeight>(maxFetching);
		
		/**
		 * Make array list of words
		 */
		int termsMT = ( null == query.planner.mustTerms) ? 0 : query.planner.mustTerms.size();
		int termsOT = ( null == query.planner.optionalTerms) ?
			0 : query.planner.optionalTerms.size();
		byte[][] wordsB = new byte[termsMT + termsOT][];
		for ( int i=0; i<termsMT; i++) {
			wordsB[i] = new Storable(query.planner.mustTerms.get(i).wordOrig).toBytes();
		}
		for ( int i=0; i<termsOT; i++) {
			wordsB[i+termsMT] = new Storable(query.planner.optionalTerms.get(i).wordOrig).toBytes();
		}
		
		TeaserFilter tf = new TeaserFilter(wordsB, (short) teaserCutSection);
		for ( int i=0; i< maxFetching; i++) {
			DocMetaWeight metaWt =  (DocMetaWeight) res.sortedDynamicWeights[i];
			byte[] idB = metaWt.id.getBytes();
			List<NVBytes> flds = 
				HReader.getCompleteRow(IOConstants.TABLE_PREVIEW, idB,tf);
			weightedTeasers.add(new DocTeaserWeight(idB, flds,metaWt.weight));
		}
		res.teasers = weightedTeasers.toArray();
		DocTeaserWeight.sort(res.teasers);
		return true;
	}
	
	public boolean commit() throws ApplicationFault, SystemFault {
		return true;
	}

	public PipeOut getInstance() {
		return this;
	}

	public boolean init(Configuration conf) throws ApplicationFault, SystemFault {
		this.DEFAULT_PAGE_SIZE = conf.getInt("page.fetch.limit", 10);
		this.DEFAULT_TEASER_LENGTH = conf.getInt("teaser.words.count", 100);
		return true;
	}
	
	
	public String getName() {
		return "BuildTeaser";
	}		
}
