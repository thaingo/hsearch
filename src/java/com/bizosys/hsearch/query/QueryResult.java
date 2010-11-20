package com.bizosys.hsearch.query;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;

import com.bizosys.oneline.ApplicationFault;


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
		Writer spw = new StringWriter();
		
		try {
			if ( null == this.sortedStaticWeights) {
				spw.write("Sorted Static Weights = 0");
			} else {
				spw.write("Sorted Static Weights : " + this.sortedStaticWeights.length);
			}
			
			if ( null == this.sortedDynamicWeights) {
				spw.write("Sorted Dynamic Weights = 0");
			} else {
				spw.write("Sorted Dynamic Weights : " + this.sortedDynamicWeights.length);
			}

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
