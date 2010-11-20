package com.bizosys.hsearch.outpipe;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import com.bizosys.ferrari.TestFerrari;
import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.index.Term;
import com.bizosys.hsearch.inpipe.util.StopwordManager;
import com.bizosys.hsearch.lang.Stemmer;
import com.bizosys.hsearch.query.HQuery;
import com.bizosys.hsearch.query.QueryContext;
import com.bizosys.hsearch.query.QueryPlanner;
import com.bizosys.hsearch.query.QueryTerm;
import com.bizosys.hsearch.util.GeoId;

public class LuceneQueryParserTest extends TestCase {
	
	public static void main(String[] args) throws Exception {
		new StopwordManager();
		List<String> stopwords = new ArrayList<String>();
		stopwords.add("but");
		StopwordManager.getInstance().setLocalStopwords(stopwords);

		LuceneQueryParserTest t = new LuceneQueryParserTest();
		
        TestFerrari.testAll(t);
	}

	public void testNoType(String query) throws Exception  {
		query = query.trim().replace(" ", "");

		QueryContext ctx = new QueryContext(query);
		QueryPlanner qp = parseQuery(ctx);
		
		QueryTerm queryTerm = qp.mustTerms.get(0);
		String stemmedQ = Stemmer.getInstance().stem(query);
		assertEquals(stemmedQ, queryTerm.wordStemmed);
		assertEquals(Term.NO_TERM_TYPE, queryTerm.termType);
	}

	public void testSpecificType(String type, String value) throws Exception  {
		type = type.trim().replace(" ", "");
		value = value.trim().replace(" ", "");
		
		String query = type + ":" + value;
		QueryContext ctx = new QueryContext(query);
		QueryPlanner qp = parseQuery(ctx);
		QueryTerm queryTerm = qp.mustTerms.get(0);
		assertEquals(type, queryTerm.termType);
		assertEquals(Stemmer.getInstance().stem(value), queryTerm.wordStemmed);
	}
	
	public void testAnd() throws Exception  {
		QueryContext ctx = new QueryContext("+abinash +karan");
		QueryPlanner qp = parseQuery(ctx);
		assertEquals("abinash", qp.mustTerms.get(0).wordStemmed);
		assertEquals("karan", qp.mustTerms.get(1).wordStemmed);
		assertEquals(Term.NO_TERM_TYPE, qp.mustTerms.get(0).termType);
	}	

	public void testOr() throws Exception  {
		QueryContext ctx = new QueryContext("abinash karan");
		QueryPlanner qp = parseQuery(ctx);
		QueryTerm queryTerm = qp.optionalTerms.get(0);
		assertEquals("abinash", queryTerm.wordStemmed);
		assertEquals("karan", qp.optionalTerms.get(1).wordStemmed);
		assertEquals(Term.NO_TERM_TYPE, queryTerm.termType);
		assertEquals(Term.NO_TERM_TYPE, queryTerm.termType);
	}	

	public void testMultiphrase() throws Exception  {
		QueryContext ctx = new QueryContext("+abinash_karan");
		QueryPlanner qp = parseQuery(ctx);
		QueryTerm queryTerm = qp.mustTerms.get(0);
		assertEquals("abinash karan", queryTerm.wordStemmed);
		assertEquals(Term.NO_TERM_TYPE, queryTerm.termType);
	}
	
	/**
	public void testMultiphraseQuotes() throws Exception  {
		QueryContext ctx = new QueryContext("\\\"abinash karan\\\"");
		QueryPlanner qp = parseQuery(ctx);
		QueryTerm queryTerm = qp.mustTerms.get(0);
		System.out.println("INSTR : [" +  queryTerm.wordStemmed + "");
		assertEquals("abinash karan", queryTerm.wordStemmed);
		assertEquals(Term.NO_TERM_TYPE, queryTerm.termType);
	}	
	*/
	

	/**
	 * Does not support ^ &*()<>:/{}[]` 
	 * @throws Exception
	 */
	public void testSpecial() throws Exception  {
		QueryContext ctx = new QueryContext("abinash !@#$%,.?';+-_");
		QueryPlanner qp = parseQuery(ctx);
		assertEquals("abinash", qp.optionalTerms.get(0).wordStemmed);
		
		QueryTerm queryTerm = qp.optionalTerms.get(1);
		assertEquals("@#$%,.?';+-", queryTerm.wordStemmed);
	}
	
	public void testStopword() throws Exception  {
		QueryContext ctx = new QueryContext("abinash but");
		QueryPlanner qp = parseQuery(ctx);
		assertEquals(1, qp.mustTerms.size());
		assertEquals(null, qp.optionalTerms);
		
		assertEquals("abinash", qp.mustTerms.get(0).wordStemmed);
		assertEquals(Term.NO_TERM_TYPE, qp.mustTerms.get(0).termType);
	}		

	public void testTermUnderscore() throws Exception  {
		QueryContext ctx = new QueryContext("personal_loan");
		QueryPlanner planner = parseQuery(ctx);
		assertEquals( 1, planner.mustTerms.size());
		assertEquals( "personal loan", planner.mustTerms.get(0).wordStemmed);
	}
	
	public void testMultiphraseWithType() throws Exception  {
		QueryContext ctx = new QueryContext("+title:The_Right_Way");
		QueryPlanner planner = parseQuery(ctx);
		assertEquals( 1, planner.mustTerms.size());
		assertEquals( "title", planner.mustTerms.get(0).termType);
		assertEquals( "the right wa", planner.mustTerms.get(0).wordStemmed);
	}
	
	public void testTypeAndNonTypeMixed() throws Exception  {
		QueryContext ctx = new QueryContext("id:560083 abinash");
		QueryPlanner planner = parseQuery(ctx);
		assertEquals( 2, planner.optionalTerms.size());
		assertEquals( null, planner.mustTerms);
		assertEquals( "id", planner.optionalTerms.get(0).termType);
		assertEquals( "560083", planner.optionalTerms.get(0).wordStemmed);
		assertEquals( "abinash", planner.optionalTerms.get(1).wordStemmed);
	}
	
	public void testDocId() throws Exception  {
		QueryContext ctx = new QueryContext("olid:560083 abinash");
		parseQuery(ctx);
		assertEquals("560083", ctx.id);
	}
	

	public void testDocumentTypeFilter() throws Exception  {
		QueryContext ctx = new QueryContext("typ:client abinash");
		parseQuery(ctx);
		assertEquals("client", ctx.docType);
	}

	public void testDocumentStateFilter() throws Exception  {
		QueryContext ctx = new QueryContext("ste:_active abinash");
		parseQuery(ctx); //It is in the stop word.. So just a work around.
		assertEquals("active", Storable.getString(ctx.state.toBytes()) );
	}
	
	public void testOrgunitFilter() throws Exception  {
		QueryContext ctx = new QueryContext("tenant:icici abinash");
		parseQuery(ctx); 
		assertEquals("icici", Storable.getString(ctx.tenant.toBytes()) );
	}

	public void testBornBeforeFilter() throws Exception  {
		QueryContext ctx = new QueryContext("createdb:1145353454334 abinash");
		parseQuery(ctx); 
		assertEquals(1145353454334L, ctx.createdBefore.longValue());
	}

	public void testBornAfterFilter() throws Exception  {
		QueryContext ctx = new QueryContext("createda:2165353454334 abinash");
		parseQuery(ctx); 
		assertEquals(2165353454334L, ctx.createdAfter.longValue());
	}

	public void testTouchAfterFilter() throws Exception  {
		QueryContext ctx = new QueryContext("modifieda:1145353454334 abinash");
		parseQuery(ctx); 
		assertEquals(1145353454334L, ctx.modifiedAfter.longValue());
	}

	public void testTouchBeforeFilter() throws Exception  {
		QueryContext ctx = new QueryContext("modifiedb:2165353454334 abinash");
		parseQuery(ctx); 
		assertEquals(2165353454334L, ctx.modifiedBefore.longValue());
	}
	
	public void testAreaInKmRadiusFilter() throws Exception  {
		QueryContext ctx = new QueryContext("aikr:12 abinash");
		parseQuery(ctx); 
		assertEquals(12, ctx.areaInKmRadius);
	}

	public void testMetaFieldFilter() throws Exception  {
		QueryContext ctx = new QueryContext("mf:_type abinash");
		parseQuery(ctx); 
		assertEquals("type", ctx.metaFields[0]);
	}

	public void testMatchIpFilter() throws Exception  {
		QueryContext ctx = new QueryContext("matchip:192.168.12.23 abinash");
		parseQuery(ctx); 
		assertEquals("192.168.12.23", ctx.matchIp);
	}

	public void testLatitudeFilter() throws Exception  {
		GeoId geoId = GeoId.convertLatLng(12.78f, 98.78f);
		QueryContext ctx = new QueryContext("latlng:12.78,98.78 abinash");
		parseQuery(ctx);
		assertNotNull(ctx.getGeoId().getHouse(), geoId.getHouse());
	}

	public void testScoreBoosterOnDocWeight() throws Exception  {
		QueryContext ctx = new QueryContext("docbst:40 abinash");
		parseQuery(ctx);
		assertEquals(40, ctx.boostDocumentWeight);
	}

	public void testScoreBoosterOnIPProximity() throws Exception  {
		QueryContext ctx = new QueryContext("ipbst:40 abinash");
		parseQuery(ctx); 
		assertEquals(40, ctx.boostIpProximity);
	}

	public void testScoreBoosterOnAuthorProximity() throws Exception  {
		QueryContext ctx = new QueryContext("ownerbst:40 abinash");
		parseQuery(ctx); 
		assertEquals(40, ctx.boostOwner);
	}

	public void testScoreBoosterOnFreshness() throws Exception  {
		QueryContext ctx = new QueryContext("freshbst:40 abinash");
		parseQuery(ctx); 
		assertEquals(40, ctx.boostFreshness);
	}

	public void testScoreBoosterOnPreciousness() throws Exception  {
		QueryContext ctx = new QueryContext("preciousbst:40 abinash");
		parseQuery(ctx); 
		assertEquals(40, ctx.boostPrecious);
	}

	public void testScoreBoosterOnSocialRanking() throws Exception  {
		QueryContext ctx = new QueryContext("choicebst:40 abinash");
		parseQuery(ctx); 
		assertEquals(40, ctx.boostChoices);
	}

	public void testBodyFetchLimit() throws Exception  {
		QueryContext ctx = new QueryContext("dfl:25 abinash");
		parseQuery(ctx); 
		assertEquals(25, ctx.documentFetchLimit);
	}	
	public void testMetaFetchLimit() throws Exception  {
		QueryContext ctx = new QueryContext("mfl:200 abinash");
		parseQuery(ctx); 
		assertEquals(200, ctx.metaFetchLimit);
	}

	public void testFacetFetchLimit() throws Exception  {
		QueryContext ctx = new QueryContext("ffl:3000 abinash");
		parseQuery(ctx); 
		assertEquals(3000, ctx.facetFetchLimit);
	}
	
	public void testTeaserLength() throws Exception  {
		QueryContext ctx = new QueryContext("tsl:240 abinash");
		parseQuery(ctx); 
		assertEquals(240, ctx.teaserSectionLen);
	}
	
	public void testSortOnMetaSingle() throws Exception  {
		QueryContext ctx = new QueryContext("som:orgUnit=asc abinash");
		parseQuery(ctx); 
		assertEquals(1, ctx.sortOnMeta.size());
		assertEquals(QueryContext.SORT_ASC, ctx.sortOnMeta.get("orgUnit"));
	}		

	public void testSortOnMetaMultiple() throws Exception  {
		QueryContext ctx = new QueryContext("som:orgUnit=asc som:type=desc abinash");
		parseQuery(ctx); 
		assertEquals(2, ctx.sortOnMeta.size());
		assertEquals(QueryContext.SORT_ASC, ctx.sortOnMeta.get("orgUnit"));
		assertEquals(QueryContext.SORT_DESC, ctx.sortOnMeta.get("type"));
	}		
	
	public void testSortOnXmlFieldSingle() throws Exception  {
		QueryContext ctx = new QueryContext("sof:age=asc abinash");
		parseQuery(ctx); 
		assertEquals(1, ctx.sortOnFld.size());
		assertEquals(QueryContext.SORT_ASC, ctx.sortOnFld.get("age"));
	}		

	public void testSortOnXmlFieldMultiple() throws Exception  {
		QueryContext ctx = new QueryContext("sof:age=asc sof:name=desc abinash");
		parseQuery(ctx); 
		assertEquals(2, ctx.sortOnFld.size());
		assertEquals(QueryContext.SORT_ASC, ctx.sortOnFld.get("age"));
		assertEquals(QueryContext.SORT_DESC, ctx.sortOnFld.get("name"));
	}
	
	public void testSortMixedMetaAndXml() throws Exception  {
		QueryContext ctx = new QueryContext("som:orgUnit=asc sof:age=asc abinash");
		parseQuery(ctx); 
		assertEquals(1, ctx.sortOnFld.size());
		assertEquals(1, ctx.sortOnMeta.size());
		assertEquals(QueryContext.SORT_ASC, ctx.sortOnFld.get("age"));
		assertEquals(QueryContext.SORT_ASC, ctx.sortOnMeta.get("orgUnit"));
	}	

	public void testClusterUsingNLP() throws Exception  {
		QueryContext ctx = new QueryContext("cluster:body=nlp cluster:url=nlp abinash");
		parseQuery(ctx); 
		assertEquals(2, ctx.cluster.size());
		assertEquals(QueryContext.CLUSTER_NLP, ctx.cluster.get("body"));
		assertEquals(QueryContext.CLUSTER_NLP, ctx.cluster.get("url"));
	}		
	
	public void testClusterOnMeta() throws Exception  {
		QueryContext ctx = new QueryContext("cluster:from=meta cluster:to=meta abinash");
		parseQuery(ctx); 
		assertEquals(2, ctx.cluster.size());
		assertEquals(QueryContext.CLUSTER_META, ctx.cluster.get("from"));
		assertEquals(QueryContext.CLUSTER_META, ctx.cluster.get("to"));
	}		

	public void testClusterOnXmlField() throws Exception  {
		QueryContext ctx = new QueryContext("cluster:empname=structure abinash");
		parseQuery(ctx); 
		assertEquals(1, ctx.cluster.size());
		assertEquals(QueryContext.CLUSTER_STRUCTURE, ctx.cluster.get("empname"));
	}		

	public void testClusterMixed() throws Exception  {
		QueryContext ctx = new QueryContext("cluster:empname=structure cluster:from=meta cluster:body=nlp abinash");
		parseQuery(ctx); 
		assertEquals(3, ctx.cluster.size());
		assertEquals(QueryContext.CLUSTER_META, ctx.cluster.get("from"));
		assertEquals(QueryContext.CLUSTER_NLP, ctx.cluster.get("body"));
		assertEquals(QueryContext.CLUSTER_STRUCTURE, ctx.cluster.get("empname"));
	}
	
	public void testTouchStone() throws Exception  {
		QueryContext ctx = new QueryContext("ts:1 abinash");
		QueryPlanner qp = parseQuery(ctx);
		assertEquals(true, ctx.isTouchStone);

		ctx = new QueryContext("abinash");
		qp = parseQuery(ctx);
		assertEquals(false, ctx.isTouchStone);
	
	}
	

	private QueryPlanner parseQuery(QueryContext ctx) throws Exception {
		QueryPlanner planner = new QueryPlanner();
		HQuery query = new HQuery(ctx, planner);
		
		new LuceneQueryParser().visit(query);
		//System.out.println( "Query Planner \n" + planner);
		//System.out.println( "Query Context \n" + ctx);
		return planner;
	}
	
	
}
