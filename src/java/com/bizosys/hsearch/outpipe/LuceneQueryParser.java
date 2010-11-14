package com.bizosys.hsearch.outpipe;

import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.oneline.ApplicationFault;
import org.apache.oneline.SystemFault;
import org.apache.oneline.conf.Configuration;
import org.apache.oneline.pipes.PipeOut;

import com.bizosys.hsearch.query.HQuery;
import com.bizosys.hsearch.query.IMatch;
import com.bizosys.hsearch.query.LuceneQueryAnalyzer;
import com.bizosys.hsearch.query.QueryContext;
import com.bizosys.hsearch.query.QueryPlanner;
import com.bizosys.hsearch.query.QueryTerm;
import com.bizosys.hsearch.query.ReserveQueryWord;
import com.bizosys.hsearch.util.LuceneConstants;

/**
 * We are supporting lucene query parser
 * @author karan
 *
 */
public class LuceneQueryParser implements PipeOut{
	
	/**
	 * Hashcode of the class names
	 */
	public static final int BooleanQueryH = -1627092240; //BooleanQuery;
	public static final int TermQueryH = 1505675340; //TermQuery;
	public static final int MultiTermQueryH = 828881907; //MultiTermQuery;
	public static final int MultiPhraseQueryH = 189896710; //MultiPhraseQuery;
	public static final int PhraseQueryH = 2048324127; //PhraseQuery;
	public static final int FuzzyQueryH = -144373170; //FuzzyQuery;
	public static final int WildcardQueryH = 212911006; //WildcardQuery;
	public static final int PrefixQueryH = -1806737946; //PrefixQuery;
	public static final int TermRangeQueryH = 2054197383; //TermRangeQueryH;
	public static final int NumericRangeQueryH  = 21621000; //NumericRangeQuery ;
	public static final int SpanQueryH  = -1164824530; //SpanQuery ;
	
	
	public LuceneQueryParser() {
	}	

	public boolean visit(Object objQuery) throws ApplicationFault, SystemFault {
		HQuery query = (HQuery) objQuery;
		QueryContext ctx = query.ctx;
		QueryPlanner planner = query.planner;
		
		if ( null == ctx || null == ctx.queryString) {
			throw new ApplicationFault("Blank Query "); 
		}
		
		QueryParser qp = new QueryParser( LuceneConstants.version,
			com.bizosys.hsearch.index.Term.NO_TERM_TYPE, 
			new LuceneQueryAnalyzer());
		
		if ( L.l.isDebugEnabled() )
			L.l.debug("Query String = " + ctx.queryString);
		
		Query q = null;
		try {
			q = qp.parse(ctx.queryString);
		} catch (ParseException ex) {
			throw new ApplicationFault("Provide a valid search query"); 
		} catch (Exception ex) {
			L.l.fatal(ex);
			L.l.debug("planner.LuceneQueryParser LuceneQueryAnalyzer failed > ", ex);
			throw new ApplicationFault( "Query parsing error, " + ctx.queryString); 
		}
		parseQuery(ctx,planner,q, true);
		return true;
	}
	
	/**
	 * Does an recurssion to find the must terms and optional terms
	 * @param hq
	 * @param q
	 * @param isRequired
	 */
	public void parseQuery ( QueryContext hq, QueryPlanner planner,
		Query q, boolean isRequired) throws ApplicationFault {
		
		int queryTypeH = q.getClass().getName().hashCode();
		
		switch (queryTypeH) {
			case  BooleanQueryH:
				if ( L.l.isDebugEnabled() ) L.l.debug("LuceneQueryParser > BooleanQuery");
				BooleanQuery bq = (BooleanQuery) q;
		    	BooleanClause[] clauses = bq.getClauses();
		    	for ( int i=0; i< clauses.length; i++) {
		    		parseQuery(hq,planner,clauses[i].getQuery(), clauses[i].isRequired());
		    	}
		    	break;

			case TermQueryH:	
				if ( L.l.isDebugEnabled() ) L.l.debug("LuceneQueryParser > TermQuery");
		   		TermQuery tq = (TermQuery) q;
		   		populateQueryTerm(hq, planner, IMatch.EQUAL_TO, isRequired, tq.getTerm());
		   		break;
			
			case PhraseQueryH:
				if ( L.l.isDebugEnabled() ) L.l.debug("LuceneQueryParser > PhraseQuery");
		   		PhraseQuery kk = (PhraseQuery) q;
		    	org.apache.lucene.index.Term[] terms = kk.getTerms();
		    	for (Term term : terms) {
		    		populateQueryTerm(hq, planner, IMatch.EQUAL_TO, isRequired, term);
				} 
		    	break;
			
			case FuzzyQueryH:
				if ( L.l.isDebugEnabled() ) L.l.debug("LuceneQueryParser > FuzzyQuery");

				//This is handled in do you mean this..
		   		FuzzyQuery fq = (FuzzyQuery) q;
		   		populateQueryTerm(hq, planner, IMatch.PATTERN_MATCH, isRequired, fq.getTerm());
		   		break;
		   		
			case 	WildcardQueryH:
				if ( L.l.isDebugEnabled() ) L.l.debug("LuceneQueryParser > WildcardQuery");
		   		WildcardQuery wq = (WildcardQuery) q;
		   		populateQueryTerm(hq, planner, IMatch.PATTERN_MATCH, isRequired, wq.getTerm());
		   		break;
				
			case 	PrefixQueryH:
				if ( L.l.isDebugEnabled() ) L.l.debug("LuceneQueryParser > PrefixQuery");
		   		PrefixQuery pq = (PrefixQuery) q;
		   		populateQueryTerm(hq, planner, IMatch.STARTS_WITH, isRequired, pq.getPrefix());
		   		break;
		   		
			case TermRangeQueryH:
				if ( L.l.isDebugEnabled() ) L.l.debug("LuceneQueryParser > ConstantScoreRangeQuery");
				TermRangeQuery trq = (TermRangeQuery) q;
		   		populateQueryTerm(hq, planner, IMatch.RANGE, isRequired, trq);
		   		break;

			case NumericRangeQueryH:
				if ( L.l.isDebugEnabled() ) L.l.debug("LuceneQueryParser > ConstantScoreRangeQuery");
				NumericRangeQuery nrq = (NumericRangeQuery) q;
		   		populateQueryTerm(hq, planner, IMatch.RANGE, isRequired, nrq);
		   		break;

			default: 
				L.l.warn("Missed LuceneQueryParsing - " + queryTypeH);
		}
	}
	
	/**
	 * Cast this to oneline query term. 
	 * TODO:// For the comma separated terms run the with in query..
	 * @param hq
	 * @param matchingType
	 * @param isRequired
	 * @param aTerm
	 */
	private void populateQueryTerm(QueryContext hq, QueryPlanner planner,
		int matchingType, boolean isRequired, Term aTerm)
		throws ApplicationFault {
		
		String fldName = aTerm.field();
		String fldValue = aTerm.text();
		int reserveWord = ReserveQueryWord.getInstance().mapReserveWord(fldName);
		
		if ( ReserveQueryWord.NO_RESERVE_WORD == reserveWord) {
			
			/**
			 * This is a regular term.. Now we will work on the fldValue
			 */
			QueryTerm term = new QueryTerm();
			term.setTerm(fldName, fldValue, matchingType);
			
			hq.totalTerms++;
			if ( isRequired ) planner.addMustTerm(term);
			else planner.addOptionalTerm(term);
	
		} else {
			hq.populate(reserveWord, fldValue);
		}
	}
	
	/**
	 * A range. Currently with in is not available.
	 * @param hq
	 * @param matchingType
	 * @param isRequired
	 * @param aTerm
	 */
	private void populateQueryTerm(QueryContext hq, QueryPlanner planner,
		int matchingType, boolean isRequired, NumericRangeQuery query) throws ApplicationFault {
		
		throw new ApplicationFault("Numeric Range Query Not yet implemented");
	}
	
	private void populateQueryTerm(QueryContext hq, QueryPlanner planner,
			int matchingType, boolean isRequired, TermRangeQuery query) throws ApplicationFault {
			
			throw new ApplicationFault("Term Range Query Not yet implemented");
	}
	
	
	public static void main(String[] args) {
		System.out.println(BooleanQueryH == "com.bizosys.lucene.search.BooleanQuery".hashCode());
		System.out.println(TermQueryH ==  "com.bizosys.lucene.search.TermQuery".hashCode());
		System.out.println(MultiTermQueryH ==  "com.bizosys.lucene.search.MultiTermQuery".hashCode());
		System.out.println(WildcardQueryH ==  "com.bizosys.lucene.search.WildcardQuery".hashCode());
		System.out.println(PhraseQueryH ==  "com.bizosys.lucene.search.PhraseQuery".hashCode());
		System.out.println(PrefixQueryH ==  "com.bizosys.lucene.search.PrefixQuery".hashCode());
		System.out.println(MultiPhraseQueryH ==  "com.bizosys.lucene.search.MultiPhraseQuery".hashCode());
		System.out.println(FuzzyQueryH ==  "com.bizosys.lucene.search.FuzzyQuery".hashCode());
		System.out.println(TermRangeQueryH ==  "com.bizosys.lucene.search.TermRangeQuery".hashCode());
		System.out.println(NumericRangeQueryH ==  "com.bizosys.lucene.search.NumericRangeQuery".hashCode());
		System.out.println(SpanQueryH ==  "com.bizosys.lucene.search.SpanQuery".hashCode());
	}

	public boolean commit() throws ApplicationFault, SystemFault {
		return true;
	}

	public PipeOut getInstance() {
		return this;
	}

	public boolean init(Configuration conf) throws ApplicationFault, SystemFault {
		return false;
	}
}
