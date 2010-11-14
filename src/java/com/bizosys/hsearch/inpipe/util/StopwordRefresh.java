package com.bizosys.hsearch.inpipe.util;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.StopFilter;
import org.apache.oneline.ApplicationFault;
import org.apache.oneline.SystemFault;
import org.apache.oneline.services.batch.BatchTask;
import org.apache.oneline.util.StringUtils;

import com.bizosys.hsearch.common.IStorable;
import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.hbase.HReader;
import com.bizosys.hsearch.hbase.HWriter;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.inpipe.L;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.hsearch.util.RecordScalar;

public class StopwordRefresh implements BatchTask {
	
	private static final char STOPWORD_SEPARATOR = '\t';
	private static byte[] STOP_WORD_LISTS_KEY = "STOP_WORDS".getBytes();

	public String getJobName() {
		return "StopWordRefresh";
	}

	public Object process() throws ApplicationFault, SystemFault {
		NV nv = new NV(IOConstants.NAME_VALUE_BYTES, IOConstants.NAME_VALUE_BYTES);
		RecordScalar scalar = new RecordScalar(STOP_WORD_LISTS_KEY,nv);
		HReader.getScalar(IOConstants.TABLE_CONFIG, scalar);
		if ( null != scalar.kv.data) {
			String words = new String(scalar.kv.data.toBytes());
			List<String> wordLst = StringUtils.fastSplit(words, STOPWORD_SEPARATOR);
			Set<String> stopWordsTemp = StopwordManager.getInstance().stopWords;
			StopwordManager.getInstance().stopWords = buildStopwords(wordLst);
			stopWordsTemp.clear();
			stopWordsTemp = null;
		}
		return null;
	}

	public void setJobName(String arg0) {
	}
	
	/**
	 * This refreshes the stopword list.
	 * @param allStopWords
	 * @return
	 * @throws ApplicationFault
	 */
	@SuppressWarnings("unchecked")
	private Set<String> buildStopwords(List<String> allStopWords) throws ApplicationFault {
		
		if ( null == allStopWords) {
			L.l.warn(" FilterStopWords: No stop words." );
			return null;
		}
		
		try {
			Set wordSet = StopFilter.makeStopSet(allStopWords);
			if (L.l.isInfoEnabled()) { 
				L.l.info(" StopwordManager: stopWords.size - " + wordSet.size());
			}
			return (Set<String>) wordSet;

		} catch (Exception ex) {
			throw new ApplicationFault(ex);
		}
	}
	
	public static void add(List<String> lstWord) throws SystemFault{
		if ( null == lstWord) return;
		IStorable wordB = new Storable(
			StringUtils.listToString(lstWord, STOPWORD_SEPARATOR));
		NV nv = new NV(
			IOConstants.NAME_VALUE_BYTES, IOConstants.NAME_VALUE_BYTES,wordB);
		RecordScalar scalar = new RecordScalar(STOP_WORD_LISTS_KEY,nv);
		try {
			HWriter.insertScalar(IOConstants.TABLE_CONFIG, scalar, true);
		} catch (IOException ex) {
			L.l.fatal("StopwordRefresh > ", ex);
			throw new SystemFault(ex);
		}
		
	}
}
