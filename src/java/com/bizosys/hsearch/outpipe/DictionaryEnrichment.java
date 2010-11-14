package com.bizosys.hsearch.outpipe;

import java.util.List;

import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeOut;

import com.bizosys.hsearch.dictionary.DictEntry;
import com.bizosys.hsearch.dictionary.DictionaryManager;
import com.bizosys.hsearch.query.HQuery;
import com.bizosys.hsearch.query.QueryPlanner;
import com.bizosys.hsearch.query.QueryTerm;

public class DictionaryEnrichment implements PipeOut{
	
	public DictionaryEnrichment() {
	}	

	public boolean visit(Object objQuery) throws ApplicationFault, SystemFault {
		HQuery query = (HQuery) objQuery;
		//QueryContext ctx = query.ctx;
		QueryPlanner planner = query.planner;
		
		loadFromDictionary(planner.mustTerms);
		loadFromDictionary(planner.optionalTerms);
		
		return true;
	}

	private void loadFromDictionary(List<QueryTerm> queryWordL) throws ApplicationFault {
		if ( null == queryWordL) return;
		for (QueryTerm term : queryWordL) {
			DictEntry entry = 
				DictionaryManager.getInstance().get(term.wordStemmed);
			term.foundTerm = entry;
		}
	}
	
	public boolean commit() throws ApplicationFault, SystemFault {
		return true;
	}

	public PipeOut getInstance() {
		return this;
	}

	public boolean init(Configuration conf) throws ApplicationFault, SystemFault {
		return true;
	}
}
