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
import com.bizosys.hsearch.inpipe.ComputeTokens;
import com.bizosys.hsearch.inpipe.DeleteFromDictionary;
import com.bizosys.hsearch.inpipe.DeleteFromIndex;
import com.bizosys.hsearch.inpipe.DeleteFromPreviewAndDetail;
import com.bizosys.hsearch.inpipe.FilterDuplicateId;
import com.bizosys.hsearch.inpipe.FilterLowercase;
import com.bizosys.hsearch.inpipe.FilterStem;
import com.bizosys.hsearch.inpipe.FilterStopwords;
import com.bizosys.hsearch.inpipe.FilterTermLength;
import com.bizosys.hsearch.inpipe.SaveToDetail;
import com.bizosys.hsearch.inpipe.SaveToDictionary;
import com.bizosys.hsearch.inpipe.SaveToIndex;
import com.bizosys.hsearch.inpipe.SaveToPreview;
import com.bizosys.hsearch.inpipe.TokenizeStandard;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;
import com.bizosys.oneline.util.StringUtils;

/**
 * 
 * @author karan
 *
 */
public class IndexWriter {

	private static IndexWriter singleton = null;
	public static IndexWriter getInstance() {
		if ( null != singleton) return singleton;
		synchronized (IndexWriter.class) {
			if ( null != singleton) return singleton;
			singleton = new IndexWriter();
		}
		return singleton;
	}
	
	private Map<String, PipeIn> writePipes = null; 

	/**
	 * Initializes the standard pipes
	 * Default private constructor
	 */
	private IndexWriter() {
	}
	
	public void init(Configuration conf) throws SystemFault, ApplicationFault{
		if ( null == writePipes) createPipes();
		for (PipeIn pipe: writePipes.values()) {
			pipe.init(conf);
		}
	}
	
	/**
	 * Creates standard sets of pipes
	 */
	private void createPipes() {
		if ( null != this.writePipes) return;
		
		this.writePipes = new HashMap<String, PipeIn>();
		
		FilterDuplicateId fdi = new FilterDuplicateId();
		this.writePipes.put(fdi.getName(), fdi);
		
		TokenizeStandard ts = new TokenizeStandard();
		this.writePipes.put(ts.getName(), ts);

		FilterStopwords fs = new FilterStopwords();
		this.writePipes.put(fs.getName(), fs);

		FilterTermLength ftl = new FilterTermLength();
		this.writePipes.put(ftl.getName(), ftl);

		FilterLowercase flc = new FilterLowercase();
		this.writePipes.put(flc.getName(), flc);

		FilterStem fstem = new FilterStem();
		this.writePipes.put(fstem.getName(), fstem);

		ComputeTokens ct = new ComputeTokens();
		this.writePipes.put(ct.getName(), ct);

		SaveToIndex si = new SaveToIndex();
		this.writePipes.put(si.getName(), si);

		SaveToDictionary sd = new SaveToDictionary();
		this.writePipes.put(sd.getName(), sd);

		SaveToPreview stp = new SaveToPreview();
		this.writePipes.put(stp.getName(), stp);

		SaveToDetail std = new SaveToDetail();
		this.writePipes.put(std.getName(), std);

		DeleteFromIndex dfi = new DeleteFromIndex();
		this.writePipes.put(dfi.getName(), dfi);

		DeleteFromPreviewAndDetail dfpd = new DeleteFromPreviewAndDetail();
		this.writePipes.put(dfpd.getName(), dfpd);

		DeleteFromDictionary dfd = new DeleteFromDictionary();
		this.writePipes.put(dfd.getName(), dfd);
		
	}
	
	public List<PipeIn> getPipes(String stepNames) throws ApplicationFault {
		L.l.debug("IndexWriter: getPipes  = " + stepNames);
		if ( null == this.writePipes) createPipes();
		String[] steps = StringUtils.getStrings(stepNames, ",");
		List<PipeIn> anvils = new ArrayList<PipeIn>(steps.length);
		for (String step : steps) {
			PipeIn aPipe = writePipes.get(step).getInstance();
			if ( null == aPipe) {
				L.l.error("IndexWriter: getPipes Pipe not found =  " + step);
				throw new ApplicationFault("Pipe Not Found: " + step);
			}
			anvils.add(aPipe);
		}
		return anvils;
	}
	
	/**
	 * Following pipes are included in the standard write channel
	 * 
	 * FilterDuplicateId,TokenizeStandard,FilterStopwords,
	 * FilterTermLength,FilterLowercase,FilterStem,ComputeTokens,
	 * SaveToIndex,SaveToDictionary,SaveToPreview,SaveToDetail
	 * @return
	 */
	public List<PipeIn> getStandardPipes() throws ApplicationFault{
		if ( null == this.writePipes) createPipes();
		return getPipes(
			"FilterDuplicateId,TokenizeStandard,FilterStopwords,"+
			"FilterTermLength,FilterLowercase,FilterStem,ComputeTokens," +
			"SaveToIndex,SaveToDictionary,SaveToPreview,SaveToDetail");
	}
	
	/**
	 * Insert one document applying the standard pipes 
	 * @param hdoc
	 * @throws ApplicationFault
	 * @throws SystemFault
	 */
	public void insert(HDocument hdoc) throws ApplicationFault, SystemFault{
		List<PipeIn> localPipes = getStandardPipes();
		insert(hdoc,localPipes);
	}
	
	/**
	 * Insert a document with custom pipeline
	 * @param hdoc
	 * @param localPipes
	 * @throws ApplicationFault
	 * @throws SystemFault
	 */
	public void insert(HDocument hdoc, List<PipeIn> localPipes) throws ApplicationFault, SystemFault{
		
		Doc doc = new Doc(hdoc);
		L.l.info("Insert Step 1 > Value parsing is over.");
		
		for (PipeIn in : localPipes) {
			L.l.debug("IndexWriter.insert.visitting : " + in.getName());
			in.visit(doc);
		}
		L.l.info("Insert Step 2 >  Pipe processing is over.");
		
		for (PipeIn in : localPipes) {
			L.l.debug("IndexWriter.insert.comitting :" + in.getName());
			in.commit();
		}
		L.l.info("Insert Step 3 >  Commit is over.");

	}

	/**
	 * Insert bunch of documents with standard pipelines
	 * @param hdocs
	 * @throws ApplicationFault
	 * @throws SystemFault
	 */
	public void insert(List<HDocument> hdocs) throws ApplicationFault, SystemFault{
		List<PipeIn> localPipes = getStandardPipes();
		insert(hdocs,localPipes);
	}
	
	/**
	 * Insert bunch of documents with custom pipeline
	 * @param hdocs
	 * @param pipes
	 * @throws ApplicationFault
	 * @throws SystemFault
	 */
	public void insert(List<HDocument> hdocs, List<PipeIn> pipes) throws ApplicationFault, SystemFault{
		if ( null == hdocs) return;
		
		List<Doc> docs = new ArrayList<Doc>(hdocs.size());
		for (HDocument hdoc : hdocs) {
			Doc doc = new Doc(hdoc);
			docs.add(doc);
		}
		L.l.info("Insert Step 1 > Value parsing is over.");
		
		for (Doc doc : docs) {
			for (PipeIn in : pipes) {
				L.l.debug("IndexWriter.insert.visitting : " + in.getName());
				in.visit(doc);
			}
		}
		L.l.info("Insert Step 2 >  Pipe processing is over.");
		
		for (PipeIn in : pipes) {
			L.l.debug("IndexWriter.insert.comitting :" + in.getName());
			in.commit();
		}
		L.l.info("Insert Step 3 >  Commit is over.");
	}
	
	/**
	 * 1 : Load the original document
	 * 2 : Parse the document 
	 * 2 : Remove From Dictionry, Index, Preview and Detail  
	 */
	public boolean delete(String documentId) throws ApplicationFault, SystemFault {
		
		L.l.info("IndexWriter.delete : " + documentId );
		
		Doc origDoc = IndexReader.getInstance().get(documentId);
		if ( null == origDoc.teaser) return false;
		if ( null != origDoc.content) {
			if ( null != origDoc.content.stored ) {
				origDoc.content.analyzedIndexed = origDoc.content.stored;
			}
		}

		List<PipeIn> deletePipe = getPipes(
			"TokenizeStandard,FilterStopwords,FilterTermLength," +
			"FilterLowercase,FilterStem,ComputeTokens," +
			"DeleteFromIndex,DeleteFromPreviewAndDetail,DeleteFromDictionary");
		
		L.l.info("Delete Step 1 > Value parsing is over.");
		
		for (PipeIn in : deletePipe) {
			L.l.debug("IndexWriter.delete.visit : " + in.getName());
			in.visit(origDoc);
		}
		
		L.l.info("Delete Step 2 >  Pipe processing is over.");
		for (PipeIn in : deletePipe) {
			L.l.debug("IndexWriter.delete.commit : " + in.getName());
			in.commit();
		}
		L.l.info("Delete Step 3 >  Commit is over.");
		return true;
	}
}
