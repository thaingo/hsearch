package com.bizosys.hsearch.query;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;
import com.bizosys.oneline.services.ServiceFactory;

import com.bizosys.hsearch.common.HDocument;
import com.bizosys.hsearch.common.HField;
import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.hbase.HReader;
import com.bizosys.hsearch.hbase.NVBytes;
import com.bizosys.hsearch.index.IndexWriter;
import com.bizosys.hsearch.inpipe.ComputeTokens;
import com.bizosys.hsearch.inpipe.FilterLowercase;
import com.bizosys.hsearch.inpipe.FilterStem;
import com.bizosys.hsearch.inpipe.FilterStopwords;
import com.bizosys.hsearch.inpipe.FilterTermLength;
import com.bizosys.hsearch.inpipe.SaveToDetail;
import com.bizosys.hsearch.inpipe.SaveToDictionary;
import com.bizosys.hsearch.inpipe.SaveToIndex;
import com.bizosys.hsearch.inpipe.SaveToPreview;
import com.bizosys.hsearch.inpipe.TokenizeStandard;
import com.bizosys.hsearch.inpipe.util.StopwordManager;
import com.bizosys.hsearch.outpipe.ComputeStaticRanking;
import com.bizosys.hsearch.outpipe.LuceneQueryParser;
import com.bizosys.hsearch.outpipe.QuerySequencing;
import com.bizosys.hsearch.outpipe.SequenceProcessor;
import com.bizosys.hsearch.schema.IOConstants;

public class BucketDocIdFinderTest extends TestCase {
	
	public static void main(String[] args) throws Exception {
		new StopwordManager();
		List<String> stopwords = new ArrayList<String>();
		stopwords.add("but");
		StopwordManager.getInstance().setLocalStopwords(stopwords);

		Configuration conf = new Configuration();
		ServiceFactory.getInstance().init(conf, null);

		ServiceFactory.getInstance();
		BucketDocIdFinderTest t = new BucketDocIdFinderTest();
		
        //TestFerrari.testRandom(t);
		//t.populateDevelopers();
		t.testWeighing();
	}

	public void testIndexRead() throws Exception  {

		QueryContext ctx = new QueryContext("+Bizosys +Sunil Abinash");
		QueryPlanner qp = processQuery(ctx);
		
		if ( null != qp.mustTerms) {
			for (QueryTerm term : qp.mustTerms) {
				System.out.println("Must :" + term.foundIds.values().toString());
			}
		}
		if ( null != qp.optionalTerms) {
			for (QueryTerm term : qp.optionalTerms) {
				System.out.println("Optional :" + term.foundIds.values().toString());
			}
		}
	}
	
	public void testWeighing() throws Exception  {

		QueryContext ctx = new QueryContext("+Bizosys Sunil +Abinash");
		QueryPlanner qp = processQuery(ctx);
		
		for ( String key : ctx.docweight.keySet() ) {
			System.out.println(key + " : " + ctx.docweight.get(key).toString());

			List<NVBytes> values = HReader.getCompleteRow(IOConstants.TABLE_CONTENT, key.getBytes());
			if ( null == values) continue;
			for (NVBytes bytes : values) {
				System.out.println(bytes.toString());
			}
 		}
	}

	public void populateDevelopers() throws Exception {
		
		HDocument d1 = addDeveloper(new Long(1), 
			(short)1, "Developer #1", "Abinasha Karana", "Abinasha Karana, Bizosys Technologies Limited");
			
		HDocument d2 = addDeveloper(new Long(1), 
			(short)2, "Developer #2", "Sunil Guttula", "Sunil Gutulla, Bizosys Technologies Limited");

		HDocument d3 = addDeveloper(new Long(2), 
			(short)1, "Developer #3", "Jyoti Karana", "Jyoti Patra, Oracle Inc.");
				
		HDocument d4 = addDeveloper(new Long(1), 
			(short)2, "Developer #1", "Roshni", "Roshni Gutulla, House Wife");
		
		
		List<PipeIn> pipes = getStandardPipes();
		List<HDocument> docs = new ArrayList<HDocument>();
		docs.add(d1);docs.add(d2); docs.add(d3);docs.add(d4);
		
		
		new IndexWriter().insert(docs, pipes, 20);

	}

	private HDocument addDeveloper(Long bucketId, Short docPos, String id, String title, String body) throws ApplicationFault, SystemFault {
		HDocument hdoc = new HDocument();
		hdoc.originalId = new Storable(id);
		hdoc.title = new Storable(title);
		hdoc.fields = new ArrayList<HField>();
		HField fld = new HField("BODY", body);
		hdoc.fields.add(fld);
		
		return hdoc;
	}
	
	private QueryPlanner processQuery(QueryContext ctx) throws Exception {
		QueryPlanner planner = new QueryPlanner();
		HQuery query = new HQuery(ctx, planner);
		
		new LuceneQueryParser().visit(query);
		new QuerySequencing().visit(query);
		new SequenceProcessor().visit(query);
		new ComputeStaticRanking().visit(query); 
		//new ComputeStaticRanking().visit(query);
		//System.out.println( "Query Planner \n" + planner);
		//System.out.println( "Query Context \n" + ctx);
		return planner;
	}
	
	private List<PipeIn> getStandardPipes() {
		List<PipeIn> pipes = new ArrayList<PipeIn>();
		pipes.add(new TokenizeStandard());
		pipes.add(new FilterStopwords());
		pipes.add(new FilterTermLength());
		pipes.add(new FilterLowercase());
		pipes.add(new FilterStem());
		pipes.add(new ComputeTokens());
		pipes.add(new SaveToIndex());
		pipes.add(new SaveToDictionary());
		pipes.add(new SaveToPreview());
		pipes.add(new SaveToDetail());
		//pipes.add(new PrintStdout());
		return pipes;
	}
	
	

}
