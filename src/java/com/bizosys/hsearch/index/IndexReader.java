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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.bizosys.hsearch.common.HDocument;
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
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;
import com.bizosys.oneline.pipes.PipeOut;
import com.bizosys.oneline.util.StringUtils;

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
	
	private Map<String, PipeOut> readPipes = null; 

	private IndexReader() {
	}
	
	/**
	 * Creates standard sets of pipes
	 */
	private void createPipes() {
		if ( null != this.readPipes) return;
		
		this.readPipes = new HashMap<String, PipeOut>();
		
		LuceneQueryParser lqp = new LuceneQueryParser();
		this.readPipes.put(lqp.getName(), lqp);
		
		DictionaryEnrichment de = new DictionaryEnrichment();
		this.readPipes.put(de.getName(), de);

		ComputePreciousness cp = new ComputePreciousness();
		this.readPipes.put(cp.getName(), cp);

		ComputeTypeCodes ctc = new ComputeTypeCodes();
		this.readPipes.put(ctc.getName(), ctc);
		
		QuerySequencing qs = new QuerySequencing();
		this.readPipes.put(qs.getName(), qs);

		SequenceProcessor sp = new SequenceProcessor();
		this.readPipes.put(sp.getName(), sp);

		ComputeStaticRanking csr = new ComputeStaticRanking();
		this.readPipes.put(csr.getName(), csr);

		CheckMetaInfo cmi = new CheckMetaInfo();
		this.readPipes.put(cmi.getName(), cmi);
	
		ComputeDynamicRanking cdr = new ComputeDynamicRanking();
		this.readPipes.put(cdr.getName(), cdr);

		BuildTeaser bt = new BuildTeaser();
		this.readPipes.put(bt.getName(), bt);

	}	
	
	public void init(Configuration conf) throws SystemFault, ApplicationFault{
		if ( null == readPipes) createPipes();
		for (PipeOut pipe: readPipes.values()) {
			pipe.init(conf);
		}
	}
	
	/**
	 * Comma separates Steps
	 * @param stepNames
	 * @return
	 * @throws ApplicationFault
	 */
	public List<PipeOut> getPipes(String stepNames) throws ApplicationFault {
		L.l.debug("IndexReader: getPipes =  " + stepNames);
		if ( null == this.readPipes) createPipes();
		String[] steps = StringUtils.getStrings(stepNames, ",");
		List<PipeOut> anvils = new ArrayList<PipeOut>(steps.length);
		for (String step : steps) {
			PipeOut aPipe = readPipes.get(step).getInstance();
			if ( null == aPipe) {
				L.l.error("IndexReader: getPipes Pipe not found =  " + step);
				throw new ApplicationFault("Pipe Not Found: " + step);
			}
			anvils.add(aPipe);
		}
		return anvils;
	}	
	
	public List<PipeOut> getStandardPipes() throws ApplicationFault {
		if ( null == this.readPipes) createPipes();
		return getPipes(
			"LuceneQueryParser,DictionaryEnrichment,ComputePreciousness,"+
			"ComputeTypeCodes,QuerySequencing,SequenceProcessor," +
			"ComputeStaticRanking,CheckMetaInfo,ComputeDynamicRanking,BuildTeaser");
	}

	/**
	 * Get the document detail based on supplied document ID.
	 * @param origId
	 * @return
	 * @throws ApplicationFault
	 * @throws SystemFault
	 */
	public Doc get(String origId) throws ApplicationFault, SystemFault{
		return new Doc(origId);
	}
	
	/**
	 * Read the index and allows the processing to go through steps 
	 * @param ctx
	 * @return
	 * @throws ApplicationFault
	 * @throws SystemFault
	 */
	public QueryResult search(QueryContext ctx) throws ApplicationFault, SystemFault{
		return search(ctx, getStandardPipes());		
	}
	
	public QueryResult search(QueryContext ctx, List<PipeOut> pipes) throws ApplicationFault, SystemFault{
		QueryPlanner planner = new QueryPlanner();
		HQuery query = new HQuery(ctx, planner);
		
		for (PipeOut outPipe : pipes) {
			if ( ! outPipe.visit(query) ) { 
				throw new ApplicationFault("Pipe processing failed :" + outPipe.getClass().getName());
			}
		}
		return query.result;
	}
	
	
	public void insert(HDocument hdoc, List<PipeIn> localPipes) throws ApplicationFault, SystemFault{
	}
	
	public List<InvertedIndex> getInvertedIndex(long bucketId) throws ApplicationFault, SystemFault{
		
		List<NVBytes> nvs = TermTables.get(bucketId);
		if ( null == nvs) return null;
		List<InvertedIndex> iiL = new ArrayList<InvertedIndex>(nvs.size()); 
		
		for (NVBytes nv : nvs) {
			if ( null == nv.data) continue;
			List<InvertedIndex> indexes = InvertedIndex.read(nv.data);
			if ( null == indexes) continue;
			iiL.addAll(indexes);
		}
		return iiL;
	}	
	
}
