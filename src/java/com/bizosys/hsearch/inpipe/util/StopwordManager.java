package com.bizosys.hsearch.inpipe.util;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.oneline.ApplicationFault;
import org.apache.oneline.SystemFault;
import org.apache.oneline.conf.Configuration;
import org.apache.oneline.services.Request;
import org.apache.oneline.services.Response;
import org.apache.oneline.services.Service;
import org.apache.oneline.services.ServiceMetaData;
import org.apache.oneline.services.scheduler.ExpressionBuilder;
import org.apache.oneline.services.scheduler.ScheduleTask;

import com.bizosys.hsearch.inpipe.L;

public class StopwordManager implements Service{
	
	protected Set<String> stopWords = null;
	protected Set<String> EMPTY_WORDS = new HashSet<String>(1);
	ScheduleTask scheduledRefresh = null;
	
	private static StopwordManager instance = null;
	public static StopwordManager getInstance() throws ApplicationFault {
		if ( null == instance) throw new ApplicationFault(
			"StopwordManager is not initialized");
		return instance;
	}
	
	public StopwordManager() {
		instance = this;
	}
	
	public String getName() {
		return "StopwordManager";
	}

	/**
	 * Launches dictionry refresh task and initializes the dictionry.
	 */
	public boolean init(Configuration conf, ServiceMetaData arg1) {
		StopwordRefresh refreshTask = new StopwordRefresh();

		int refreshInteral = conf.getInt("stopword.refresh", 30);
		ExpressionBuilder expr = new ExpressionBuilder();
		expr.setMinute(refreshInteral, true);
		
		long startTime = new Date().getTime() + 10 * 60 * 1000 /** After 10 minutes */;
		try {
			refreshTask.process();
			scheduledRefresh = new ScheduleTask(refreshTask, expr.getExpression(), 
				new Date(startTime), new Date(Long.MAX_VALUE));
			L.l.info("StopwordManager > Stopword Refresh task is scheduled.");
			return true;
		} catch (Exception ex) {
			L.l.fatal("StopwordManager Initialization failed >", ex);
			return false;
		}
	}

	public void process(Request arg0, Response arg1) {
	}

	public void stop() {
		if ( null != this.scheduledRefresh) 
			this.scheduledRefresh.endDate = new Date(System.currentTimeMillis());
	}	

	public Set<String> getStopwords() {
		if ( null == stopWords) return EMPTY_WORDS; 
		return stopWords;
	}

	/**
	 * Set a new stopword list. This also refreshes the existing list.
	 * @param words
	 * @throws SystemFault
	 */
	public void setStopwords(List<String> words) throws SystemFault {
		StopwordRefresh.add(words);
		Set<String> newStopWords = new HashSet<String>();
		newStopWords.addAll(words);
		Set<String> stopWordsTemp = this.stopWords;
		this.stopWords = newStopWords;
		
		stopWordsTemp.clear();
		stopWordsTemp = null;
	}
	
	/**
	 * This modifies local stopwords only
	 * @param newStopWords
	 * @throws SystemFault
	 */
	public void setLocalStopwords(List<String> words) throws SystemFault {
		Set<String> newStopWords = new HashSet<String>();
		newStopWords.addAll(words);
		this.stopWords = newStopWords;
	}
	
}
