package com.bizosys.hsearch.inpipe.util;

import java.io.IOException;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

import com.bizosys.hsearch.lang.Stemmer;

public class StemFilterWrap extends TokenFilter {

	private Stemmer stemmer;
	private TermAttribute termA = null; 
	

	public StemFilterWrap(TokenStream in) {
		super(in);
		stemmer = Stemmer.getInstance();
		this.termA = (TermAttribute)in.getAttribute(TermAttribute.class);
	}
	
	public final boolean incrementToken() throws IOException {
		boolean isIncremented = input.incrementToken();
		if ( ! isIncremented ) return isIncremented;
		
		 if (termA != null) {
			 String stemWord = stemmer.stem(termA.term());
			 this.termA.setTermBuffer(stemWord);
		}
		 return isIncremented;
	}
	
	
}
