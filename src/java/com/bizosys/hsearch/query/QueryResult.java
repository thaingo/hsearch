package com.bizosys.hsearch.query;


public class QueryResult {
	public Object[] sortedStaticWeights = null; //Object = DocWeight
	public Object[] sortedDynamicWeights = null; //DocMetaWeight + weight is adjusted
	public Object[] teasers = null; //DocTeaserWeight + weight is adjusted
}
