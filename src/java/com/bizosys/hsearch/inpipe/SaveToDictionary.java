package com.bizosys.hsearch.inpipe;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.oneline.ApplicationFault;
import org.apache.oneline.SystemFault;
import org.apache.oneline.conf.Configuration;
import org.apache.oneline.pipes.PipeIn;

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
		if ( null == entries) return true;
		DictionaryManager s = DictionaryManager.getInstance();
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
