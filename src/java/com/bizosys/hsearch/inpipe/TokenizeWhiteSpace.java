package com.bizosys.hsearch.inpipe;

import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.oneline.ApplicationFault;
import org.apache.oneline.SystemFault;
import org.apache.oneline.conf.Configuration;
import org.apache.oneline.pipes.PipeIn;

import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.TermStream;
import com.bizosys.hsearch.inpipe.util.ReaderType;

public class TokenizeWhiteSpace extends TokenizeBase implements PipeIn {

	public TokenizeWhiteSpace() {
		super();
	}
	
	public PipeIn getInstance() {
		return this;
	}

	public String getName() {
		return "WhitespaceAnalyzer";
	}

	public boolean init(Configuration conf) throws ApplicationFault,SystemFault {
		return true;
	}

	public boolean visit(Object docObj) throws ApplicationFault, SystemFault {
		if ( null == docObj) return false;
		Doc doc = (Doc) docObj;
		List<ReaderType> readers = super.getReaders(doc);
    	if (null == readers) return true;
		
		try {
	    	for (ReaderType reader : readers) {
	    		Analyzer analyzer = new WhitespaceAnalyzer();
	    		TokenStream stream = analyzer.tokenStream(
	    				reader.type, reader.reader);
	    		TermStream ts = new TermStream(
		    			reader.docSection, stream, reader.type); 
		    		doc.terms.addTokenStream(ts);
	    		//Note : The reader.reader stream is already closed.
			}
	    	return true;
    	} catch (Exception ex) {
    		throw new ApplicationFault(ex);
    	}
	}

	public boolean commit() throws ApplicationFault, SystemFault {
		return true;
	}
}
