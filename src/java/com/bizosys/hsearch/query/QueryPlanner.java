package com.bizosys.hsearch.query;

import java.util.ArrayList;
import java.util.List;

public class QueryPlanner {

	public StringBuilder sb = new StringBuilder();
	public String phrases = null;

	public List<QueryTerm> mustTerms = null;
	public List<QueryTerm> optionalTerms = null;
	public List<List<QueryTerm>> sequences = null;
	
	public QueryPlanner() {
		
	}
	
	public void addMustTerm(QueryTerm aTerm) {
		aTerm.isOptional = false;
		if ( null != aTerm.wordOrig) sb.append(aTerm.wordOrig).append(' ');
		if ( null == mustTerms) mustTerms = 
			new ArrayList<QueryTerm>();
		mustTerms.add(aTerm);
	}
	
	public void addOptionalTerm(QueryTerm aTerm) {
		aTerm.isOptional = true;
		if ( null != aTerm.wordOrig) sb.append(aTerm.wordOrig).append(' ');
		if ( null == optionalTerms) optionalTerms = 
			new ArrayList<QueryTerm>();
		optionalTerms.add(aTerm);
	}
	
	public String getQueryString() {
		if ( null == phrases) {
			if ( null == sb) return null;
			else {
				phrases = sb.toString().toLowerCase().trim();
				sb.delete(0, sb.capacity());
				sb = null;
				return phrases;
			}
		} else {
			return phrases;
		}
	}
	
	/**
	 * All items from the list will get executed as parallel query. (OR)
	 * Result Set from the parallel queries are intersected
	 * 
	 * The array of keywords inside a parallel query is run sequentially.
	 * The sequencing is done using the dictionry.  
	 * @return
	 */
	public List<String[]> getParallelTerms() {
		return null;
	}	

	
	public String getClusterQuery() {
		return this.getQueryString();
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		if ( null != mustTerms  ) {
			for (QueryTerm term : mustTerms) {
				sb.append("Must Term = ").append(term).append('\n');
			}
		}
		if ( null != optionalTerms ) {
			for (QueryTerm term : optionalTerms) {
				sb.append("Optional Term = ").append(term).append('\n');
			}
		}
		return sb.toString();
	}
	
}