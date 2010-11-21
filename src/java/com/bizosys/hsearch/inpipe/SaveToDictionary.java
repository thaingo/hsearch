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
package com.bizosys.hsearch.inpipe;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;

import com.bizosys.hsearch.dictionary.DictEntry;
import com.bizosys.hsearch.dictionary.DictionaryManager;
import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.Term;

public class SaveToDictionary implements PipeIn {

	Hashtable<String, DictEntry> entries = null;
	
	public boolean visit(Object objDoc) throws ApplicationFault, SystemFault {
		if ( null == objDoc) return true;
		Doc doc = (Doc) objDoc;
		if ( null == doc.terms) return true;
		
		List<Term> terms = doc.terms.all;
		if ( null == terms) return true;
		
		/**
		 * Remove any duplicates from the local set
		 * Multi types are taken care
		 */
		Map<String, DictEntry> localSet = 
			new Hashtable<String, DictEntry>(terms.size());
		for (Term term : terms) {
			DictEntry de = null;
			if (localSet.containsKey(term.term)) {
				if ("".equals(term.termType)) continue;
				de = localSet.get(term.term);
				de.addType(term.termType);
			} else {
				de = new DictEntry(term.term, term.termType,1);
			}
			localSet.put(term.term, de);
		}
		
		/**
		 * Add the local set to merged one
		 */
		if ( null == entries) entries = new Hashtable<String, DictEntry>();
		for (String word : localSet.keySet()) {
			if ( entries.containsKey(word)) {
				entries.get(word).fldFreq++;
			} else entries.put(word, localSet.get(word));
		}
		return true;
	}

	/**
	 * Aggregate and commit all the records at one shot.
	 */
	public boolean commit() throws ApplicationFault, SystemFault {
		L.l.info("SaveToDictonary:commit()");
		if ( null == entries) return true;
		DictionaryManager s = DictionaryManager.getInstance();
		if ( L.l.isDebugEnabled() ) {
			StringBuilder sb = new StringBuilder();
			sb.append("SaveToDictonary:commit() :");
			for (DictEntry entry: entries.values()) {
				sb.append('\n').append(entry);
			}
			L.l.debug( sb.toString());
			sb.delete(0, sb.capacity());
		}
		
		s.add(entries);
		return true;
	}

	public boolean init(Configuration conf) throws ApplicationFault, SystemFault {
		return true;
	}

	public PipeIn getInstance() {
		return new SaveToDictionary();
	}

	public String getName() {
		return "SaveToDictionary";
	}

}
