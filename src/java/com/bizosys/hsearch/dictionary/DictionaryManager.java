package com.bizosys.hsearch.dictionary;

import java.util.Date;
import java.util.Hashtable;
import java.util.List;

import org.apache.oneline.ApplicationFault;
import org.apache.oneline.SystemFault;
import org.apache.oneline.conf.Configuration;
import org.apache.oneline.services.Request;
import org.apache.oneline.services.Response;
import org.apache.oneline.services.Service;
import org.apache.oneline.services.ServiceMetaData;
import org.apache.oneline.services.scheduler.ExpressionBuilder;
import org.apache.oneline.services.scheduler.ScheduleTask;
import org.apache.oneline.util.StringUtils;

public class DictionaryManager implements Service{
	
	ScheduleTask scheduledRefresh = null;
	Dictionary dict = null;
	
	private static DictionaryManager instance = null;
	public static final DictionaryManager getInstance() throws ApplicationFault {
		if ( null == instance) throw new ApplicationFault(
			"DisctionaryManager is not initialized");
		return instance;
	}
	
	public DictionaryManager() {
		instance = this;
	}
	
	public String getName() {
		return "Dictionarymanager";
	}

	/**
	 * Launches dictionry refresh task and initializes the dictionry.
	 */
	public boolean init(Configuration conf, ServiceMetaData arg1) {
		DictionaryRefresh refreshTask = new DictionaryRefresh();

		int refreshInteral = conf.getInt("dictionary.refresh", 30);
		ExpressionBuilder expr = new ExpressionBuilder();
		expr.setMinute(refreshInteral, true);
		
		long startTime = new Date().getTime() + 10 * 60 * 1000 /** After 10 minutes */;
		try {
			scheduledRefresh = new ScheduleTask(refreshTask, expr.getExpression(), 
				new Date(startTime), new Date(Long.MAX_VALUE));
			HLog.l.info("DisctionaryManager > Dictionry Refresh task is scheduled.");

			int mergeCount = conf.getInt("dictionary.merge.words", 1000);
			int pageSize = conf.getInt("dictionary.page.Size", 1000);
			this.dict = new Dictionary(mergeCount, pageSize);

			HLog.l.info("DisctionaryManager > Initializing the dictionry for first  time.");
			this.dict.buildTerms();
			return true;
		} catch (Exception ex) {
			HLog.l.fatal("DisctionaryManager >", ex);
			return false;
		}
	}

	public void process(Request arg0, Response arg1) {
	}

	public void stop() {
		if ( null != this.scheduledRefresh) 
			this.scheduledRefresh.endDate = new Date(System.currentTimeMillis());
	}	

	public List<String> getKeywords() throws ApplicationFault {
		if ( null == this.dict) throw new ApplicationFault("DictionryManager not initialized");
		return this.dict.getAll();
	}

	public List<String> getKeywords(String fromWord) throws ApplicationFault {
		if ( null == this.dict) throw new ApplicationFault("DictionryManager not initialized");
		return this.dict.getAll(fromWord);
	}
	
	/**
	 * Add a single entry to the dictionry
	 * @param entry
	 * @throws ApplicationFault
	 */
	public void add(DictEntry entry) throws ApplicationFault {
		if ( null == this.dict) throw new ApplicationFault("DictionryManager not initialized");
		Hashtable<String, DictEntry> entries = new Hashtable<String, DictEntry>(1);
		entries.put(entry.fldWord, entry);
		this.dict.add(entries);
	}

	/**
	 * Add bunch of entries to the dictionry
	 * @param entries
	 * @throws ApplicationFault
	 */
	public void add(Hashtable<String, DictEntry> entries) throws ApplicationFault {
		if ( null == this.dict) throw new ApplicationFault("DictionryManager not initialized");
		this.dict.add(entries);
	}
	
	public void refresh()  throws SystemFault, ApplicationFault {
		this.dict.buildTerms();		
	}
	
	/**
	 * Get directly the keyword
	 * @param keyword
	 * @return
	 * @throws ApplicationFault
	 */
	public DictEntry get(String keyword) throws ApplicationFault {
		if ( null == this.dict) throw new ApplicationFault("DictionryManager not initialized");
		
		if ( StringUtils.isEmpty(keyword)) return null;
		return this.dict.get(keyword);
	}
	
	/**
	 * Check for the right spelling for the given keyword
	 * @param keyword
	 * @return
	 * @throws ApplicationFault
	 */
	public List<String> getSpelled(String keyword) throws ApplicationFault {
		if ( null == this.dict) throw new ApplicationFault("DictionryManager not initialized");
		if ( StringUtils.isEmpty(keyword)) return null;
		return this.dict.fuzzy(keyword, 3);
	}
	
	/**
	 * Gets matching keywords for the given wildcard keyword.
	 * @param keyword
	 * @return
	 * @throws ApplicationFault
	 */
	public List<String> getWildCard(String keyword) throws ApplicationFault {
		if ( null == this.dict) throw new ApplicationFault("DictionryManager not initialized");
		if ( StringUtils.isEmpty(keyword)) return null;
		return this.dict.regex(keyword);
	}

	/**
	 * This completely removes the keywords from the dictionry
	 * @param keywords
	 * @throws ApplicationFault
	 */
	public void delete(List<String> keywords) throws ApplicationFault {
		if ( null == this.dict) throw new ApplicationFault("DictionryManager not initialized");
		this.dict.delete(keywords);
	}

	/**
	 * This removes all entries from the dictionry. 
	 * One  should be  careful before calling this function.
	 * @throws SystemFault
	 * @throws ApplicationFault
	 */
	public void deleteAll() throws SystemFault, ApplicationFault {
		if ( null == this.dict) throw new ApplicationFault("DictionryManager not initialized");
		this.dict.purge();
	}

	/**
	 * Once a document is removed, substract it's keywords from the dictionry
	 * If there are more 
	 * @param entries
	 * @throws ApplicationFault
	 */
	public void substract(Hashtable<String, DictEntry> entries) throws ApplicationFault {
		if ( null == this.dict) throw new ApplicationFault("DictionryManager not initialized");
		if ( null == entries) return;
		this.dict.substract(entries);
	}
	
}
