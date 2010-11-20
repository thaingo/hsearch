package com.bizosys.hsearch.query;

public class HQuery {
	public QueryContext ctx;
	public QueryPlanner planner;
	public QueryResult result =null;
	
	public HQuery(QueryContext ctx, QueryPlanner planner ) {
		this.ctx = ctx;
		this.planner = planner;
		this.result = new QueryResult();
	}
}
