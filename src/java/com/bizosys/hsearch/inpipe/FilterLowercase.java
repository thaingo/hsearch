package com.bizosys.hsearch.inpipe;

import java.util.List;
import java.util.Locale;

import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;

import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.DocTerms;
import com.bizosys.hsearch.index.TermStream;

public class FilterLowercase implements PipeIn {
	
	public FilterLowercase() {}

	public boolean commit() throws ApplicationFault, SystemFault {
		return true;
	}

	public PipeIn getInstance() {
		return this;
	}

	public String getName() {
		return "FilterLowerCase";
	}

	public boolean init(Configuration conf) throws ApplicationFault, SystemFault {
		return true;
	}

	public boolean visit(Object docObj) throws ApplicationFault, SystemFault {
		
		if ( null == docObj) return false;
		Doc doc = (Doc) docObj;
		DocTerms terms = doc.terms;
		if ( null == terms) return false;

		if ( ! Locale.ENGLISH.getDisplayLanguage().equals(
			doc.meta.locale.getLanguage()) )  return true;
		
		List<TermStream> streams = terms.getTokenStreams();
		if ( null == streams) return true; //Allow for no bodies
		
		for (TermStream ts : streams) {
			TokenStream stream = ts.stream;
			if ( null == stream) continue;
			stream = new LowerCaseFilter(stream);
			ts.stream = stream;
		}
		return true;
	}
}