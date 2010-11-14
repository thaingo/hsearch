package com.bizosys.hsearch.inpipe;

import java.util.List;

import org.apache.lucene.analysis.LengthFilter;
import org.apache.lucene.analysis.TokenStream;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;

import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.DocTerms;
import com.bizosys.hsearch.index.TermStream;

public class FilterTermLength implements PipeIn {
	
	public int minCharCutoff = 2;
	public int maxCharCutoff = 200;
	
	public FilterTermLength() {}

	public boolean commit() throws ApplicationFault, SystemFault {
		return true;
	}

	public PipeIn getInstance() {
		return this;
	}

	public String getName() {
		return "FilterTermLength";
	}

	public boolean init(Configuration conf) throws ApplicationFault, SystemFault {
		this.minCharCutoff = conf.getInt("word.cutoff.minimum", 2);
		this.maxCharCutoff = conf.getInt("word.cutoff.maximum", 200);
		return true;
	}

	public boolean visit(Object docObj) throws ApplicationFault, SystemFault {
		
		if ( null == docObj) return false;
		Doc doc = (Doc) docObj;
		DocTerms terms = doc.terms;
		if ( null == terms) return false;
		
		List<TermStream> streams = terms.getTokenStreams();
		if ( null == streams) return true; //Allow for no bodies
		
		for (TermStream ts : streams) {
			TokenStream stream = ts.stream;
			if ( null == stream) continue;
			stream = new LengthFilter(stream, minCharCutoff, maxCharCutoff);
			ts.stream = stream;
		}
		return true;
	}
	
}
