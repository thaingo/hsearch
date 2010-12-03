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
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import org.apache.log4j.Logger;

import com.bizosys.hsearch.common.HDocument;
import com.bizosys.hsearch.dictionary.DictEntry;
import com.bizosys.hsearch.dictionary.DictionaryManager;
import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.IndexReader;
import com.bizosys.hsearch.index.IndexWriter;
import com.bizosys.hsearch.query.QueryContext;
import com.bizosys.hsearch.query.QueryResult;
import com.bizosys.hsearch.schema.SchemaManager;
import com.bizosys.hsearch.security.WhoAmI;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.services.Request;
import com.bizosys.oneline.services.Response;
import com.bizosys.oneline.services.Service;
import com.bizosys.oneline.services.ServiceMetaData;
import com.bizosys.oneline.util.StringUtils;

public class DiscoveryService implements Service {

	public static Logger l = Logger.getLogger(DiscoveryService.class.getName());
	
	Configuration conf = null;
	
	public boolean init(Configuration conf, ServiceMetaData meta) {
		this.conf = conf;
		try {
			l.info("Initializing Scheme.");
			SchemaManager.getInstance().init(conf, meta);
			l.info("Scheme initialized.");
			return true;
		} catch (Exception ex) {
			l.fatal("Discovery Service Initialization Failed.");
			return false;
		}
	}

	public void stop() {
	}
	
	public String getName() {
		return "DiscoveryService";
	}	
	
	public void process(Request req, Response res) {
		
		String action = req.action; 

		try {

			if ( "index.add".equals(action) ) {
				this.doAdd(req, res);

			}  else if ( "index.add-batch".equals(action) ) {
				this.doBatchAdd(req, res);
				
			} else if ( "index.search".equals(action) ) {
				this.doSearch(req, res);

			} else if ( "index.get".equals(action) ) {
				this.doGet(req, res);
				
			} else if ( "index.delete".equals(action) ) {
				doDelete(req, res);

			}  else if ( "dictionary.lookup".equals(action) ) {
				this.lookupDictionary(req,res);
				
			}  else if ( "dictionary.list".equals(action) ) {
				this.listDictionary(req,res);
				
			}  else if ( "dictionary.add".equals(action) ) {
				this.addDictionary(req,res);

			}  else if ( "dictionary.add-batch".equals(action) ) {
				this.addBatchDictionary(req,res);
				
			}  else if ( "dictionary.spell".equals(action) ) {
				this.spell(req,res);

			}  else if ( "dictionary.regex".equals(action) ) {
				this.regex(req,res);

			}  else if ( "dictionary.delete".equals(action) ) {
				this.delete(req,res);

			}  else if ( "dictionary.delete-special".equals(action) ) {
				this.deleteSpecial(req,res);

			}  else if ( "dictionary.delete-all".equals(action) ) {
				this.deleteAll(req,res);

			} else {
				res.error("Failed Unknown operation : " + action);
			}
		} catch (Exception ix) {
			l.fatal("SearchService > ", ix);
			res.error("Failure : SearchService:" + action + " " + ix.getMessage());
		}
	}

	/**
	 * Gets a document given the {id}
	 * @param req
	 * @param res
	 * @throws ApplicationFault
	 * @throws SystemFault
	 */
	private void doGet(Request req, Response res) throws ApplicationFault, SystemFault{
		String id = req.getString("id", true, true, false);
		Doc d = IndexReader.getInstance().get(id);
		try {
			d.toXml(res.getWriter());
		} catch (IOException ex) {
			throw new SystemFault(ex);
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
	private void doAdd(Request req, Response res) 
	throws SystemFault, ApplicationFault{

		HDocument hdoc = (HDocument) req.getObject("hdoc", true);
		String runPlan = req.getString("runplan", false,true,false);
		
		if ( StringUtils.isEmpty(runPlan) ) {
			IndexWriter.getInstance().insert(hdoc);
		} else {
			IndexWriter.getInstance().insert(hdoc, 
				IndexWriter.getInstance().getPipes(runPlan));
		}
		res.writeXmlString("OK");
	}
	
	/**
	 * Update a specific Field
	 * @param req
	 * @throws Exception
	 * @throws ApplicationFault
	 * @throws ParseException
	 * @throws ApplicationError
	 */
	@SuppressWarnings("unchecked")
	private void doBatchAdd(Request req, Response res) 
	throws SystemFault, ApplicationFault {
		List<HDocument> hdocs = req.getList("hdocs", true);
		String runPlan = req.getString("runplan", false,true,false);
		
		if ( StringUtils.isEmpty(runPlan) ) {
			IndexWriter.getInstance().insert(hdocs);
		} else {
			IndexWriter.getInstance().insert(hdocs, 
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
	private void doDelete(Request req, Response res) throws SystemFault, ApplicationFault{
		String id = req.getString("id", true, true, false);
		IndexWriter.getInstance().delete(id);
		res.writeXmlString("OK");
	}

	/**
	 * Searches for a query
	 * @param req
	 * @param res
	 * @throws ApplicationError
	 * @throws ApplicationFault
	 * @throws IOException
	 */
	private void doSearch(Request req, Response res) 
		throws ApplicationFault, SystemFault { 

		String query = req.getString("query", true, true, false);
		QueryContext ctx = new QueryContext(query);
		if ( null != req.user) ctx.user = (WhoAmI) req.user;
		QueryResult results = null;
		results = IndexReader.getInstance().search(ctx);
		if ( null != results) {
			results.toXml(res.getWriter());
			return;
		}
		res.writeXmlString("<r>none</r>");
	}
	
	private void lookupDictionary(Request req, Response res) 
	throws ApplicationFault, SystemFault {
		String word = req.getString("word", true, true, false);
		DictEntry entry = DictionaryManager.getInstance().get(word);
		if ( null == entry ) {
			res.writeXmlString("<r>none</r>");
			return;
		}
		
		try {
			entry.toXml(res.getWriter());
		} catch (IOException ex) {
			throw new SystemFault(ex);
		}
	}
	
	private void listDictionary(Request req, Response res) 
	throws ApplicationFault, SystemFault {
		String startWord = req.getString("word", false, true, true);
		
		List<String> words = null;
		if ( StringUtils.isEmpty(startWord) ) 
			words = DictionaryManager.getInstance().getKeywords();
		else words = DictionaryManager.getInstance().getKeywords(startWord);
		
		if ( null == words ) {
			res.writeXmlString("<r>none</r>");
			return;
		}

		res.writeXMLList(words);
	}
	
	private void addDictionary(Request req, Response res) 
	throws ApplicationFault, SystemFault {
		DictEntry entry = (DictEntry) req.getObject("entry", true);
		DictionaryManager.getInstance().add(entry);
		res.writeXmlString("OK");
	}
	
	@SuppressWarnings("unchecked")
	private void addBatchDictionary(Request req, Response res) 
	throws ApplicationFault, SystemFault {
		List<DictEntry> entries = (List<DictEntry>) req.getList("entries", true);
		Hashtable<String, DictEntry> hashEntries = new 
			Hashtable<String, DictEntry>(entries.size());
		
		for (DictEntry aEntry : entries) {
			hashEntries.put(aEntry.fldWord, aEntry);
		}
		DictionaryManager.getInstance().add(hashEntries);
		res.writeXmlString("OK");
	}

	private void spell(Request req, Response res) 
	throws ApplicationFault, SystemFault {
		String word = req.getString("word", true, true, false);
		List<String> words = DictionaryManager.getInstance().getSpelled(word);
		if ( null == words ) {
			res.writeXmlString("<r>none</r>");
			return;
		}
		
		res.writeXMLList(words);
	}
	
	private void regex(Request req, Response res) 
	throws ApplicationFault, SystemFault {
		String word = req.getString("word", true, true, false);
		List<String> words = DictionaryManager.getInstance().getWildCard(word);
		if ( null == words ) {
			res.writeXmlString("<r>none</r>");
			return;
		}
		
		res.writeXMLList(words);
	}

	private void delete(Request req, Response res) 
	throws ApplicationFault, SystemFault {
		String words = req.getString("words", true, true, false);
		List<String> wordL = StringUtils.fastSplit(words, ',');
		DictionaryManager.getInstance().delete(wordL);
		res.writeXmlString("OK");
	}

	private void deleteSpecial(Request req, Response res) 
	throws ApplicationFault, SystemFault {
		String word = req.getString("word", true, true, false);
		List<String> wordL = new ArrayList<String>(1);
		wordL.add(word);
		DictionaryManager.getInstance().delete(wordL);
		res.writeXmlString("OK");
	}
	
	private void deleteAll(Request req, Response res) 
	throws ApplicationFault, SystemFault {
		DictionaryManager.getInstance().deleteAll();
		res.writeXmlString("OK");
	}

}
