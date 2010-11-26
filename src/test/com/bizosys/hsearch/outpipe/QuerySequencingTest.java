/*
* Copyright 2010 The Apache Software Foundation
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.bizosys.hsearch.outpipe;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import com.bizosys.ferrari.TestFerrari;
import com.bizosys.hsearch.inpipe.util.StopwordManager;
import com.bizosys.hsearch.query.HQuery;
import com.bizosys.hsearch.query.QueryContext;
import com.bizosys.hsearch.query.QueryPlanner;
import com.bizosys.hsearch.query.QueryTerm;

public class QuerySequencingTest extends TestCase {
	
	public static void main(String[] args) throws Exception {
		new StopwordManager();
		List<String> stopwords = new ArrayList<String>();
		stopwords.add("but");
		StopwordManager.getInstance().setLocalStopwords(stopwords);

		QuerySequencingTest t = new QuerySequencingTest();
		
        TestFerrari.testRandom(t);
	}

	public void oneTerm(String query) throws Exception  {
		query = query.trim().replace(" ", "");

		QueryContext ctx = new QueryContext(query);
		QueryPlanner qp = parseQuery(ctx);
		
		List<QueryTerm> step0 = qp.sequences.get(0);
		assertEquals(1, step0.size());
	}

	public void testAndAnd(String and1, String and2) throws Exception  {
		and1 = and1.trim().replace(" ", "").toLowerCase();
		and2 = and2.trim().replace(" ", "").toLowerCase();
		
		QueryContext ctx = new QueryContext("+" + and1 + " +" + and2);
		QueryPlanner qp = parseQuery(ctx);

		List<QueryTerm> step0 = qp.sequences.get(0);
		assertEquals(1, step0.size());
		assertEquals(and1, step0.get(0).wordOrig);

		List<QueryTerm> step1 = qp.sequences.get(1);
		assertEquals(1, step1.size());
		assertEquals(and2, step1.get(0).wordOrig);
		
	}
	
	public void testAndOr(String and, String or) throws Exception  {
		and = and.trim().replace(" ", "").toLowerCase();
		or = or.trim().replace(" ", "").toLowerCase();
		
		QueryContext ctx = new QueryContext(or + " +" + and);
		QueryPlanner qp = parseQuery(ctx);

		List<QueryTerm> step0 = qp.sequences.get(0);
		assertEquals(1, step0.size());
		assertEquals(and, step0.get(0).wordOrig);

		List<QueryTerm> step1 = qp.sequences.get(1);
		assertEquals(1, step1.size());
		assertEquals(or, step1.get(0).wordOrig);
		
	}	
	
	public void testAndOrOrOr(String and, String or1, String or2, String or3) throws Exception  {
		and = and.trim().replace(" ", "").toLowerCase();
		or1 = or1.trim().replace(" ", "").toLowerCase();
		or2 = or2.trim().replace(" ", "").toLowerCase();
		or3 = or3.trim().replace(" ", "").toLowerCase();
		
		QueryContext ctx = new QueryContext(or1 + " " + or2 + " " + or3 + " +" + and);
		QueryPlanner qp = parseQuery(ctx);

		List<QueryTerm> step0 = qp.sequences.get(0);
		assertEquals(1, step0.size());
		assertEquals(and, step0.get(0).wordOrig);

		List<QueryTerm> step1 = qp.sequences.get(1);
		assertEquals(3, step1.size());
		assertEquals(or1, step1.get(0).wordOrig);
		assertEquals(or2, step1.get(1).wordOrig);
		assertEquals(or3, step1.get(2).wordOrig);
		
	}	

	public void testOrOr(String or1, String or2) throws Exception  {
		or1 = or1.trim().replace(" ", "").toLowerCase();
		or2 = or2.trim().replace(" ", "").toLowerCase();
		
		QueryContext ctx = new QueryContext(or1 + " " + or2 );
		QueryPlanner qp = parseQuery(ctx);

		List<QueryTerm> step0 = qp.sequences.get(0);
		assertEquals(2, step0.size());
		assertEquals(or1, step0.get(0).wordOrig);
		assertEquals(or2, step0.get(1).wordOrig);
	}	

	public void testAndAndAndOr(String and1, String and2, String and3, 
			String or1, String or2) throws Exception  {
		
		and1 = and1.trim().replace(" ", "").toLowerCase();
		and2 = and2.trim().replace(" ", "").toLowerCase();
		and3 = and3.trim().replace(" ", "").toLowerCase();
		or1 = or1.trim().replace(" ", "").toLowerCase();
		or2 = or2.trim().replace(" ", "").toLowerCase();
		
		String qWord = "+" + and1 + " +" + and2 + " +" + and3 + " " + or1 + " " + or2;
		System.out.println("QWORD:" + qWord);
		QueryContext ctx = new QueryContext(qWord);
		
		QueryPlanner qp = parseQuery(ctx);
		assertEquals(4, qp.sequences.size());

		List<QueryTerm> step1 = qp.sequences.get(0);
		assertEquals(1, step1.size());
		assertEquals(and1, step1.get(0).wordOrig);
		
		List<QueryTerm> step2 = qp.sequences.get(1);
		assertEquals(1, step2.size());
		assertEquals(and2, step2.get(0).wordOrig);

		List<QueryTerm> step3 = qp.sequences.get(2);
		assertEquals(1, step3.size());
		assertEquals(and3, step3.get(0).wordOrig);

		List<QueryTerm> step4 = qp.sequences.get(3);
		assertEquals(2, step4.size());
		assertEquals(or1, step4.get(0).wordOrig);
		assertEquals(or2, step4.get(1).wordOrig);
	}	

	public void testAndAndAndOr(String or1, String or2, String or3) throws Exception  {
		
		or1 = or1.trim().replace(" ", "").toLowerCase();
		or2 = or2.trim().replace(" ", "").toLowerCase();
		or3 = or3.trim().replace(" ", "").toLowerCase();
		
		String qWord = or1 + " " + or2 + " " + or3;
		System.out.println("QWORD:" + qWord);
		QueryContext ctx = new QueryContext(qWord);
		
		QueryPlanner qp = parseQuery(ctx);
		assertEquals(1, qp.sequences.size());

		List<QueryTerm> step1 = qp.sequences.get(0);
		assertEquals(3, step1.size());
		
		assertEquals(or1, step1.get(0).wordOrig);
		assertEquals(or2, step1.get(1).wordOrig);
		assertEquals(or3, step1.get(2).wordOrig);
	}	
	
	private QueryPlanner parseQuery(QueryContext ctx) throws Exception {
		QueryPlanner planner = new QueryPlanner();
		HQuery query = new HQuery(ctx, planner);
		
		new LuceneQueryParser().visit(query);
		new QuerySequencing().visit(query);
		//System.out.println( "Query Planner \n" + planner);
		//System.out.println( "Query Context \n" + ctx);
		return planner;
	}
	
	
}
