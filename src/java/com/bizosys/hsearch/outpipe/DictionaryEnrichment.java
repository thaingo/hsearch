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
		QueryPlanner planner = query.planner;
		
		loadFromDictionary(planner.mustTerms);
		loadFromDictionary(planner.optionalTerms);
		
		return true;
	}

	private void loadFromDictionary(List<QueryTerm> queryWordL) throws ApplicationFault {
		if ( null == queryWordL) return;
		for (QueryTerm term : queryWordL) {
			L.l.debug("DictionaryEnrichment > " + term.wordStemmed);
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
	
	public String getName() {
		return "DictionaryEnrichment";
	}
	
}
