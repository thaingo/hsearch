package com.bizosys.hsearch.query;

import java.io.Writer;

import com.bizosys.oneline.util.StringPrintWriter;


public class QueryResult {
	public Object[] sortedStaticWeights = null; //Object = DocWeight
	public Object[] sortedDynamicWeights = null; //DocMetaWeight + weight is adjusted
	public Object[] teasers = null; //DocTeaserWeight + weight is adjusted
	
	public void toXml(Writer out) {
	}
	
	public String toStrig(Writer out) {
		StringPrintWriter spw = new StringPrintWriter();
		toXml(spw);
		return spw.toString();
	}
	
}
