package com.bizosys.hsearch.query;

import java.io.IOException;
import java.io.Writer;

import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.util.StringPrintWriter;


public class QueryResult {
	
	public Object[] sortedStaticWeights = null; //Object = DocWeight
	public Object[] sortedDynamicWeights = null; //DocMetaWeight + weight is adjusted
	public Object[] teasers = null; //DocTeaserWeight + weight is adjusted
	
	public void toXml(Writer writer) throws ApplicationFault{
		if ( null == teasers) return;
		
		try {
			writer.append("<list>");
			int docIndex = 0;
			for (Object teaserO : this.teasers) {
				writer.append("<doc>");
				writer.append("<index>" + docIndex + "</index>");
				DocTeaserWeight dtw = (DocTeaserWeight) teaserO;
				dtw.toXml(writer);
				writer.append("</doc>");
			}
			writer.append("</list>");
		} catch (IOException e) {
			L.l.error("Error in preparing result output.", e);
			throw new ApplicationFault("QueryResult::toXml", e);
		}

	}
	
	@Override
	public String toString() {
		Writer spw = new StringPrintWriter();
		try {
			toXml(spw);
			return spw.toString();
		} catch (Exception ex) {
			return ex.getMessage();
		}
	}
	
	public void cleanup() {
		sortedStaticWeights = null;  
		this.sortedDynamicWeights = null;
		this.teasers = null;
	}
}
