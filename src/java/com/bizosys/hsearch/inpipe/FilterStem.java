package com.bizosys.hsearch.inpipe;

import java.util.List;

import org.apache.lucene.analysis.TokenStream;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;

import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.DocTerms;
import com.bizosys.hsearch.index.TermStream;
import com.bizosys.hsearch.inpipe.util.StemFilterWrap;

public class FilterStem implements PipeIn {
	
	boolean isStemming = true;
	
	public FilterStem() {}

	public boolean commit() throws ApplicationFault, SystemFault {
		return true;
	}

	public PipeIn getInstance() {
		return this;
	}

	public String getName() {
		return "FilterStem";
	}

	public boolean init(Configuration conf) throws ApplicationFault, SystemFault {
		this.isStemming = conf.getBoolean("stemming", true);
		return true;
	}

	public boolean visit(Object docObj) throws ApplicationFault, SystemFault {
		if ( ! this.isStemming ) return true;
		
		if ( null == docObj) return false;
		Doc doc = (Doc) docObj;
		DocTerms terms = doc.terms;
		if ( null == terms) return false;
		
		List<TermStream> streams = terms.getTokenStreams();
		if ( null == streams) return true; //Allow for no bodies
		
		for (TermStream ts : streams) {
			TokenStream stream = ts.stream;
			if ( null == stream) continue;
			stream = new StemFilterWrap(stream);
			ts.stream = stream;
		}
		return true;
	}
	
}
