package com.bizosys.hsearch.index;

import org.apache.lucene.analysis.TokenStream;

public class TermStream {
	public Character sighting;
	public TokenStream stream;
	public String type;
	
	public TermStream(Character sighting, TokenStream stream, String type) {
		this.sighting = sighting;
		this.stream = stream;
		this.type = type;
	}
}
