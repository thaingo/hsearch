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

package com.bizosys.hsearch;

import java.io.IOException;
import java.text.ParseException;

import org.apache.log4j.Logger;

import com.bizosys.hsearch.common.HDocument;
import com.bizosys.hsearch.index.IndexWriter;
import com.bizosys.hsearch.schema.SchemaManager;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.services.Request;
import com.bizosys.oneline.services.Response;
import com.bizosys.oneline.services.Service;
import com.bizosys.oneline.services.ServiceMetaData;
import com.bizosys.oneline.util.StringUtils;

public class SearchService implements Service {

	public static Logger l = Logger.getLogger(SearchService.class.getName());
	
	Configuration conf = null;
	
	public boolean init(Configuration conf, ServiceMetaData meta) {
		this.conf = conf;
		try {
			l.info("Initializing Search Scheme.");
			SchemaManager.getInstance().init(conf, meta);
			l.info("Search Scheme initialized.");
			return true;
		} catch (Exception ex) {
			l.fatal("Search Service Initialization Failed.");
			return false;
		}
		
	}

	public void stop() {
	}
	
	public String getName() {
		return "SearchService";
	}	
	
	public void process(Request req, Response res) {
		
		String action = req.action; 

		try {

			if ( "index".equals(action) ) {
				this.doIndex(req, res);

			}  else if ( "index.ids".equals(action) ) {
				this.doIndex(req, res);
				
			}  else if ( "look.dictionary".equals(action) ) {
				this.matchDictionary(req,res);
				
			} else if ( "search".equals(action) ) {
				this.doSearch(req, res);

			} else if ( "get".equals(action) ) {
				this.doGet(req, res);
				
			} else if ( "update".equals(action) ) {
				doUpdate(req, res);
				
			} else if ( "delete".equals(action) ) {
				doDelete(req, res);
				
			} else {
				res.error("Failed Unknown operation : " + action);
			}
		} catch (Exception ix) {
			l.fatal("SearchService > ", ix);
			res.error("Failure : SearchService:" + action + " " + ix.getMessage());
		}
	}

	/**
	 * Indexes a String. For indexing along with File Upload
	 * happens through fileuploadservlet.xml
	 * @param req
	 * @param res
	 * @throws ApplicationError
	 * @throws ApplicationFault
	 * @throws IOException
	 * @throws ParseException
	 */
	private void doIndex(Request req, Response res) 
	throws SystemFault, ApplicationFault{

		HDocument hdoc = (HDocument) req.getObject("hdoc", true);
		String runPlan = req.getString("runplan", true,true,true);
		
		if ( StringUtils.isEmpty(runPlan) ) {
			IndexWriter.getInstance().insert(hdoc);
			return;
		} else {
			IndexWriter.getInstance().insert(hdoc, 
				IndexWriter.getInstance().getPipes(runPlan));
		}
		
		res.writeXmlString("OK");
		
		
	}
	
	/**
	 * Deletes a document for the given Id.
	 * @param req
	 * @param res
	 * @throws ApplicationFault
	 */
	private void doDelete(Request req, Response res) throws IOException, ParseException {
	}

	/**
	 * Update a specific Field
	 * @param req
	 * @throws Exception
	 * @throws ApplicationFault
	 * @throws ParseException
	 * @throws ApplicationError
	 */
	private void doUpdate(Request req, Response res) throws IOException, ParseException {
	}

	/**
	 * Searches for a query
	 * @param req
	 * @param res
	 * @throws ApplicationError
	 * @throws ApplicationFault
	 * @throws IOException
	 */
	private void doSearch(Request req, Response res) throws IOException, ParseException {
	}
	
	/**
	 * Gets a codument
	 * @param req
	 * @param res
	 * @throws ApplicationError
	 * @throws ApplicationFault
	 * @throws IOException
	 */
	private void doGet(Request req, Response res) throws IOException, ParseException {
	}

	private void matchDictionary(Request req, Response res) throws IOException, ParseException {
	}
}
