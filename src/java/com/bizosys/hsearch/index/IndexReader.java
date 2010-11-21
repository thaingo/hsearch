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
package com.bizosys.hsearch.index;

import java.util.ArrayList;
import java.util.List;

import com.bizosys.hsearch.hbase.NVBytes;
import com.bizosys.hsearch.outpipe.BuildTeaser;
import com.bizosys.hsearch.outpipe.CheckMetaInfo;
import com.bizosys.hsearch.outpipe.ComputeDynamicRanking;
import com.bizosys.hsearch.outpipe.ComputePreciousness;
import com.bizosys.hsearch.outpipe.ComputeStaticRanking;
import com.bizosys.hsearch.outpipe.ComputeTypeCodes;
import com.bizosys.hsearch.outpipe.DictionaryEnrichment;
import com.bizosys.hsearch.outpipe.LuceneQueryParser;
import com.bizosys.hsearch.outpipe.QuerySequencing;
import com.bizosys.hsearch.outpipe.SequenceProcessor;
import com.bizosys.hsearch.query.HQuery;
import com.bizosys.hsearch.query.QueryContext;
import com.bizosys.hsearch.query.QueryPlanner;
import com.bizosys.hsearch.query.QueryResult;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.pipes.PipeOut;

/**
 * 
 * @author karan
 *
 */
public class IndexReader {
	
	private static IndexReader singleton = null;
	public static IndexReader getInstance() {
		if ( null != singleton) return singleton;
		synchronized (IndexReader.class) {
			if ( null != singleton) return singleton;
			singleton = new IndexReader();
		}
		return singleton;
	}
	
	private List<PipeOut> standardPipes = null;
	private IndexReader() {
		this.standardPipes = new ArrayList<PipeOut>();
		
		this.standardPipes.add(new LuceneQueryParser());
		this.standardPipes.add(new DictionaryEnrichment());
		this.standardPipes.add(new ComputePreciousness());
		this.standardPipes.add(new ComputeTypeCodes());
		this.standardPipes.add(new QuerySequencing());
		this.standardPipes.add( new SequenceProcessor());
		
		this.standardPipes.add( new ComputeStaticRanking()); 
		this.standardPipes.add( new CheckMetaInfo());
		this.standardPipes.add( new ComputeDynamicRanking());
		this.standardPipes.add( new BuildTeaser());
	}
	
	public List<PipeOut> getStandardPipes() {
		return this.standardPipes;
	}
	
	public void setStandardPipes(List<PipeOut> pipes) {
		this.standardPipes = pipes;
	}
	
	/**
	 * Read the index and allows the processing to go through steps 
	 * @param ctx
	 * @return
	 * @throws ApplicationFault
	 * @throws SystemFault
	 */
	public QueryResult search(QueryContext ctx) throws ApplicationFault, SystemFault{
		QueryPlanner planner = new QueryPlanner();
		HQuery query = new HQuery(ctx, planner);
		
		for (PipeOut outPipe : this.standardPipes) {
			if ( ! outPipe.visit(query) ) { 
				throw new ApplicationFault("Pipe processing failed :" + outPipe.getClass().getName());
			}
		}
		return query.result;
	}
	
	public Doc get(String origId) throws ApplicationFault, SystemFault{
		return new Doc(origId);
	}
	
	public List<InvertedIndex> getInvertedIndex(long bucketId) throws ApplicationFault, SystemFault{
		
		List<NVBytes> nvs = TermTables.get(bucketId);
		if ( null == nvs) return null;
		List<InvertedIndex> iiL = new ArrayList<InvertedIndex>(nvs.size()); 
		
		for (NVBytes nv : nvs) {
			iiL.addAll(InvertedIndex.read(nv.data));
		}
		return iiL;
	}	
	
}
