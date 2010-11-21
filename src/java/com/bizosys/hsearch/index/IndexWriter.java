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
import com.bizosys.oneline.pipes.PipeIn;

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
	
	private List<PipeIn> standardPipes = null;

	/**
	 * Initializes the standard pipes
	 * Default private constructor
	 */
	private IndexWriter() {
		this.standardPipes = new ArrayList<PipeIn>();
		
		this.standardPipes.add(new FilterDuplicateId());
		this.standardPipes.add(new TokenizeStandard());
		this.standardPipes.add(new FilterStopwords());
		this.standardPipes.add(new FilterTermLength());
		this.standardPipes.add(new FilterLowercase());
		this.standardPipes.add(new FilterStem());
		this.standardPipes.add(new ComputeTokens());
		this.standardPipes.add(new SaveToIndex());
		this.standardPipes.add(new SaveToDictionary());
		this.standardPipes.add(new SaveToPreview());
		this.standardPipes.add(new SaveToDetail());
	}
	
	/**
	 * If necessary create copies and keep
	 * @return
	 */
	public List<PipeIn> getStandardPipes() {
		List<PipeIn> pipes = new ArrayList<PipeIn>(this.standardPipes.size());
		for (PipeIn spipe : this.standardPipes) {
			pipes.add(spipe.getInstance());
		}
		return pipes;
	}
	
	/**
	 * Change the standard pipes across the application
	 * @param pipes
	 */
	public void setStandardPipes(List<PipeIn> pipes) {
		this.standardPipes = pipes;
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
	public void delete(String documentId) throws ApplicationFault, SystemFault {
		
		L.l.info("IndexWriter.delete : " + documentId );
		
		Doc origDoc = IndexReader.getInstance().get(documentId);
		if ( null == origDoc.teaser) 
			throw new ApplicationFault("Check permission before deletion.");

		List<PipeIn> deletePipe = new ArrayList<PipeIn>();
		
		deletePipe.add(new TokenizeStandard());
		deletePipe.add(new FilterStopwords());
		deletePipe.add(new FilterTermLength());
		deletePipe.add(new FilterLowercase());
		deletePipe.add(new FilterStem());
		deletePipe.add(new ComputeTokens());

		deletePipe.add(new DeleteFromIndex());
		deletePipe.add(new DeleteFromPreviewAndDetail());
		deletePipe.add(new DeleteFromDictionary());
		
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
	}
}
