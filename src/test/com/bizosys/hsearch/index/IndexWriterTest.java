package com.bizosys.hsearch.index;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import com.bizosys.ferrari.TestFerrari;
import com.bizosys.hsearch.common.HDocument;
import com.bizosys.hsearch.common.HField;
import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.dictionary.DictionaryManager;
import com.bizosys.hsearch.query.DocTeaserWeight;
import com.bizosys.hsearch.query.QueryContext;
import com.bizosys.hsearch.query.QueryResult;
import com.bizosys.hsearch.security.WhoAmI;
import com.bizosys.hsearch.util.FileReaderUtil;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.services.ServiceFactory;

public class IndexWriterTest extends TestCase {

	public static void main(String[] args) throws Exception {
		IndexWriterTest t = new IndexWriterTest();
		Configuration conf = new Configuration();
		ServiceFactory.getInstance().init(conf, null);
		List<String> kwL = DictionaryManager.getInstance().getDictionary().getAll();
		for (String kw : kwL) {
			System.out.println(kw.toString());
		}
		
		DictionaryManager.getInstance().deleteAll();
        //TestFerrari.testRandom(t);
		/**
		t.testIndexSingleDoc("bizosys-123", "abinash", "bangalore");
		IndexWriter.getInstance().delete("bizosys-123");		
		t.testIndexSingleDoc("bizosys-123", "abinash", "bangalore");
		t.testIndexSingleDoc("bizosys-123", "abinash", "bangalore");
		IndexWriter.getInstance().delete("bizosys-123");
		*/
		t.testIndexMultiDoc("ABC-001", "ABC-002", "sunil", "bangalore");
		t.testIndexMultiDoc("ABC-001", "ABC-002", "sunil", "bangalore");
		t.testIndexMultiDoc("ABC-001", "ABC-002", "sunil", "bangalore");
		
	}
	
	public void testIndexSingleDoc(String id, String name, String location) throws Exception {
		HDocument hdoc = new HDocument();
		hdoc.originalId = new Storable(id);
		hdoc.title = new Storable(name);
		hdoc.fields = new ArrayList<HField>();
		hdoc.fields.add(new HField("LOCATION", location));
		IndexWriter.getInstance().insert(hdoc);
		
		QueryResult res = IndexReader.getInstance().search(new QueryContext(name));
		assertEquals(1, res.teasers.length);
		DocTeaserWeight dtw = (DocTeaserWeight)res.teasers[0];
		assertEquals(id.toLowerCase(), new String(dtw.id.toBytes()));
		assertEquals(name, dtw.title.toString());
		System.out.println(res.toString());
	}
	
	public void testIndexMultiDoc(String id1,String id2,String name, String location) throws Exception {
		HDocument hdoc = new HDocument();
		hdoc.originalId = new Storable(id1);
		hdoc.title = new Storable(name);
		hdoc.fields = new ArrayList<HField>();
		hdoc.fields.add(new HField("LOCATION", location));
		IndexWriter.getInstance().insert(hdoc);
		
		HDocument hdoc2 = new HDocument();
		hdoc2.originalId = new Storable(id2);
		hdoc2.title = new Storable(name);
		hdoc2.fields = new ArrayList<HField>();
		hdoc2.fields.add(new HField("LOCATION", location));
		IndexWriter.getInstance().insert(hdoc2);
		
		QueryResult res = IndexReader.getInstance().search(new QueryContext(name));
		assertEquals(2, res.teasers.length);
		System.out.println(res.toString());
	}	
	
	
	public void testIndexVanilaInsert(String id, String title, String teaser) throws Exception {
		HDocument hdoc = new HDocument();
		hdoc.originalId = new Storable(id);
		hdoc.title = new Storable(title);
		hdoc.fields = new ArrayList<HField>();
		IndexWriter.getInstance().insert(hdoc);

		//QueryResult res = IndexReader.getInstance().search(new QueryContext(id));
		QueryResult res = IndexReader.getInstance().search(new QueryContext(title));
		assertEquals(1, res.teasers.length);
		DocTeaserWeight dtw = (DocTeaserWeight)res.teasers[0];
		assertEquals(id.toLowerCase(), new String(dtw.id.toBytes()));
		assertEquals(title, dtw.title.toString());
		System.out.println(res.toString());
	}
	
	public void testIndexFieldInsert(String id, String title, String teaser) throws Exception {
		
		HDocument hdoc = new HDocument();
		hdoc.originalId = new Storable(id);
		hdoc.title = new Storable(title);
		hdoc.fields = new ArrayList<HField>();
		HField fld = new HField("BODY",FileReaderUtil.toString("sample.txt"));
		hdoc.fields.add(fld);
		
		QueryResult res = IndexReader.getInstance().search(new QueryContext(id));
		IndexWriter.getInstance().insert(hdoc);
	}
	
	public void testIndexUpdate(String keyword1, String keyword2, String keyword3, 
			String keyword4, String keyword5, String keyword6, String keyword7,  
			String keyword8, String keyword9, String keyword10) throws Exception {
		
		String[] keywords = new String[] {
				keyword1, keyword2, keyword3, keyword4, keyword5,
				keyword6, keyword7, keyword8, keyword9, keyword10
		};

		StringBuilder sb = new StringBuilder();
		List<HDocument> hdocs = new ArrayList<HDocument>(5000); 
		for ( int i=0; i<5000; i++) {
			HDocument hdoc = new HDocument();
			hdoc.originalId = new Storable("ORIG_ID:" + i);
			hdoc.title = new Storable("TITLE:" + i);
			sb.delete(0,sb.capacity());
			for (String k : keywords) {
				sb.append(k).append(i).append(' ');		
			} 
			hdoc.fields = new ArrayList<HField>();
			HField fld = new HField("FLD1",sb.toString());
			hdoc.fields.add(fld);
			hdocs.add(hdoc);
		}
		IndexWriter.getInstance().insert(hdocs);
	}
	
	public void testIndexDelete() throws Exception{
		HDocument hdoc = new HDocument();
		hdoc.originalId = new Storable("BIZOSYS-103");
		hdoc.title = new Storable("Ram tere Ganga maili");
		hdoc.fields = new ArrayList<HField>();
		
		IndexWriter.getInstance().insert(hdoc);
		
		QueryContext ctx = new QueryContext("Ganga");
		ctx.user = new WhoAmI("n4501");
		QueryResult res = IndexReader.getInstance().search(ctx);
		System.out.println("Result:" + res.toString());
		IndexWriter.getInstance().delete("BIZOSYS-103");
	}
}
