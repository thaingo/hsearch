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
package com.bizosys.hsearch.dictionary;

import java.util.Date;
import java.util.Hashtable;
import java.util.List;

import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.services.Request;
import com.bizosys.oneline.services.Response;
import com.bizosys.oneline.services.Service;
import com.bizosys.oneline.services.ServiceMetaData;
import com.bizosys.oneline.services.scheduler.ExpressionBuilder;
import com.bizosys.oneline.services.scheduler.ScheduleTask;
import com.bizosys.oneline.util.StringUtils;

/**
 * This is the Facade for the dictionry service. It's responsible for
 * initializing dictionry service as well as serving clients. This is the
 * single entry point for outside clients to perform dictionry operations.
 * @author karan
 *
 */
public class DictionaryManager implements Service{
	
	/**
	 * Dictionry Refresh Task. In a multi machine environment, this keeps 
	 * loading the dictionry to in memory locally in interval to ensure
	 * fuzzy and regex searches.
	 */
	ScheduleTask scheduledRefresh = null;
	
	/**
	 * The dictionry instance
	 */
	Dictionary dict = null;
	
	/**
	 * Singleton
	 */
	private static DictionaryManager instance = null;
	
	/**
	 * Constructor is private. This ensures singleton
	 * @return	DictionaryManager
	 * @throws SystemFault
	 */
	public static final DictionaryManager getInstance() throws SystemFault {
		if ( null == instance) throw new SystemFault(
			"DisctionaryManager is not initialized");
		return instance;
	}
	
	/**
	 *	Default Constructor 
	 *  Needs to be initialized only once. Done by ServiceFacade.
	 */
	public DictionaryManager() {
		instance = this;
	}
	
	/**
	 * Service name - Dictionarymanager
	 */
	public String getName() {
		return "Dictionarymanager";
	}

	/**
	 * Launches dictionry refresh task and initializes the dictionry.
	 */
	public boolean init(Configuration conf, ServiceMetaData arg1) {
		DictionaryLog.l.info("Initializing Dictionary Service");
		DictionaryRefresh refreshTask = new DictionaryRefresh();

		int refreshInteral = conf.getInt("dictionary.refresh", 30);
		ExpressionBuilder expr = new ExpressionBuilder();
		expr.setMinute(refreshInteral, true);
		
		long startTime = new Date().getTime() + 10 * 60 * 1000 /** After 10 minutes */;
		try {
			scheduledRefresh = new ScheduleTask(refreshTask, expr.getExpression(), 
				new Date(startTime), new Date(Long.MAX_VALUE));
			DictionaryLog.l.info("DisctionaryManager > Dictionry Refresh task is scheduled.");

			int mergeCount = conf.getInt("dictionary.merge.words", 1000);
			int pageSize = conf.getInt("dictionary.page.Size", 1000);
			this.dict = new Dictionary(mergeCount, pageSize);

			DictionaryLog.l.info("DisctionaryManager > Initializing the dictionry for first  time.");
			this.dict.buildTerms();
			return true;
		} catch (Exception ex) {
			DictionaryLog.l.fatal("DisctionaryManager >", ex);
			return false;
		}
	}

	public void process(Request arg0, Response arg1) {
	}

	/**
	 * Stop the refresh task
	 */
	public void stop() {
		if ( null != this.scheduledRefresh) 
			this.scheduledRefresh.endDate = new Date(System.currentTimeMillis());
	}	

	
	/**
	 * Get the first page words from the dictionary
	 * @return	List of words
	 * @throws SystemFault
	 */
	public List<String> getKeywords() throws SystemFault {
		if ( null == this.dict) throw new SystemFault("DictionryManager not initialized");
		return this.dict.getAll();
	}

	public List<String> getKeywords(String fromWord) throws SystemFault {
		if ( null == this.dict) throw new SystemFault("DictionryManager not initialized");
		return this.dict.getAll(fromWord);
	}
	
	/**
	 * Add a single entry to the dictionry
	 * @param entry
	 * @throws SystemFault
	 */
	public void add(DictEntry entry) throws SystemFault {
		if ( null == this.dict) throw new SystemFault("DictionryManager not initialized");
		Hashtable<String, DictEntry> entries = new Hashtable<String, DictEntry>(1);
		entries.put(entry.fldWord, entry);
		this.dict.add(entries);
	}

	/**
	 * Add bunch of entries to the dictionry
	 * @param entries
	 * @throws SystemFault
	 */
	public void add(Hashtable<String, DictEntry> entries) throws SystemFault {
		if ( null == this.dict) throw new SystemFault("DictionryManager not initialized");
		this.dict.add(entries);
	}
	
	public void refresh()  throws SystemFault {
		this.dict.buildTerms();		
	}
	
	/**
	 * Get directly the keyword
	 * @param keyword
	 * @return	Dictionary Entry
	 * @throws SystemFault
	 */
	public DictEntry get(String keyword) throws SystemFault {
		if ( null == this.dict) throw new SystemFault("DictionryManager not initialized");
		
		if ( StringUtils.isEmpty(keyword)) return null;
		return this.dict.get(keyword);
	}
	
	/**
	 * Check for the right spelling for the given keyword
	 * @param keyword
	 * @return	List of matching words
	 * @throws SystemFault
	 */
	public List<String> getSpelled(String keyword) throws SystemFault {
		if ( null == this.dict) throw new SystemFault("DictionryManager not initialized");
		if ( StringUtils.isEmpty(keyword)) return null;
		return this.dict.fuzzy(keyword, 3);
	}
	
	/**
	 * Gets matching keywords for the given wildcard keyword.
	 * @param keyword	The regular expression
	 * @return	List of matching words
	 * @throws SystemFault
	 */
	public List<String> getWildCard(String keyword) throws SystemFault {
		if ( null == this.dict) throw new SystemFault("DictionryManager not initialized");
		if ( StringUtils.isEmpty(keyword)) return null;
		return this.dict.regex(keyword);
	}

	/**
	 * This completely removes the keywords from the dictionry
	 * @param keywords
	 * @throws SystemFault
	 */
	public void delete(List<String> keywords) throws SystemFault {
		if ( null == this.dict) throw new SystemFault("DictionryManager not initialized");
		this.dict.delete(keywords);
	}

	/**
	 * This removes all entries from the dictionry. 
	 * One  should be  careful before calling this function.
	 * @throws SystemFault
	 */
	public void deleteAll() throws SystemFault, SystemFault {
		if ( null == this.dict) throw new SystemFault("DictionryManager not initialized");
		this.dict.purge();
	}

	/**
	 * Once a document is removed, substract it's keywords from the dictionry
	 * If there are more 
	 * @param entries
	 * @throws SystemFault
	 */
	public void substract(Hashtable<String, DictEntry> entries) throws SystemFault {
		if ( null == this.dict) throw new SystemFault("DictionryManager not initialized");
		if ( null == entries) return;
		this.dict.substract(entries);
	}
	
	public Dictionary getDictionary() {
		return this.dict;
	}
	
}
