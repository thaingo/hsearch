package com.bizosys.hsearch.query;

import java.io.Reader;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.apache.lucene.analysis.standard.StandardFilter;
import com.bizosys.oneline.ApplicationFault;

import com.bizosys.hsearch.inpipe.util.StopwordManager;
import com.bizosys.hsearch.outpipe.L;

public class LuceneQueryAnalyzer extends Analyzer {

	public LuceneQueryAnalyzer() {
	}
	
	public final TokenStream tokenStream(String fieldName, Reader reader) {
        WhitespaceTokenizer tokenStream = new WhitespaceTokenizer(reader);
        TokenStream result = new StandardFilter(tokenStream);
        boolean ignoreCase = true;
        try {
        	final Set<String> stopWords = StopwordManager.getInstance().getStopwords();
        	if ( null != stopWords) { 
           		result = new StopFilter(true,result, stopWords,ignoreCase);
        	}
        } catch (ApplicationFault ex) {
        	L.l.fatal("LuceneQueryAnalyzer > tokensteam" , ex);
        }
        return result;
    }
}
