package com.bizosys.hsearch.index;

import java.util.ArrayList;
import java.util.List;

public class DocTerms {

	private List<TermStream> tokenStreams = null;
	public List<Term> all = null;

	public List<Term> getTermList() {
		if ( null != all) return all;
		all = new ArrayList<Term>(100);
		return all;
	}
	
	public List<TermStream> getTokenStreams() {
		return this.tokenStreams;
	}

	/**
	 * At which location this stream was found..
	 * @param name
	 * @param modifiedStream
	 */
	public void addTokenStream(TermStream stream) {
		if ( L.l.isDebugEnabled())
			L.l.debug("DocTerms > Adding token stream - " + stream.sighting + " - " + stream.type);
		if ( null == tokenStreams) 
			tokenStreams = new ArrayList<TermStream>();
		
		tokenStreams.add(stream);		
	}

	/**
	 * Release all the held resources. Recycles this object for the
	 * next processing.
	 */
	public void cleanup() {
		try { 
			if ( null != this.tokenStreams) {
				for (TermStream tstream: this.tokenStreams) {
					tstream.stream.close();
				}
				this.tokenStreams.clear();
			}
		} catch (Exception ex) {
			L.l.warn("DocTerms:cleanup: ", ex);
		}
	}
}
