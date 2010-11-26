package com.bizosys.hsearch.index;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import junit.framework.TestCase;

import com.bizosys.ferrari.TestFerrari;
import com.bizosys.hsearch.common.HDocument;
import com.bizosys.hsearch.common.HField;
import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.dictionary.DictionaryManager;
import com.bizosys.hsearch.inpipe.util.StopwordManager;
import com.bizosys.hsearch.inpipe.util.StopwordRefresh;
import com.bizosys.hsearch.query.DocTeaserWeight;
import com.bizosys.hsearch.query.DocWeight;
import com.bizosys.hsearch.query.QueryContext;
import com.bizosys.hsearch.query.QueryResult;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.services.ServiceFactory;
import com.sun.jersey.api.core.ApplicationAdapter;


public class IndexReaderTest extends TestCase {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		ServiceFactory.getInstance().init(conf, null);
		
		IndexReaderTest t = new IndexReaderTest();
        //TestFerrari.testRandom(t);
		t.testCreatedBefore("CURIE");
		//System.out.println(DictionaryManager.getInstance().getKeywords().toString());
		//System.out.println(DictionaryManager.getInstance().get("hydrogen") );		
	}

	public void testGet(String id, String title) throws Exception {
		String content = "Welcome to bizosys technologies";
		
		HDocument doc1 = new HDocument();
		doc1.originalId = id;
		doc1.title = "Title : " + title ;
		doc1.cacheText = doc1.title + " " + content;
		doc1.fields = new ArrayList<HField>();
		HField fld = new HField("BODY", content);
		doc1.fields.add(fld);
		IndexWriter.getInstance().insert(doc1);

		Doc d = IndexReader.getInstance().get(id);
		assertNotNull(d);
		assertEquals(new String(d.teaser.id.toBytes()), id);
		assertEquals(new String(d.teaser.title.toBytes()), doc1.title);
		assertEquals(new String(d.teaser.cacheText.toBytes()), doc1.cacheText);
		assertEquals(d.content.stored.size(), 1);

		IndexWriter.getInstance().delete(id);
	}
	
	public void testVanillaSearch(String id, String title) throws Exception  {
		HDocument doc1 = new HDocument();
		doc1.originalId = "Id : " + id ;
		doc1.title = "Title : " + title;
		IndexWriter.getInstance().insert(doc1);
		QueryContext ctx = new QueryContext(title);
		QueryResult res = IndexReader.getInstance().search(ctx);
		
		assertNotNull(res.teasers);
		assertEquals(1, res.teasers.length);
		DocTeaserWeight teaser = (DocTeaserWeight) res.teasers[0];
		assertEquals(new String(teaser.id.toBytes()), doc1.originalId);
		assertEquals(new String(teaser.title.toBytes()), doc1.title);
		IndexWriter.getInstance().delete(doc1.originalId);
	}

	public void testNullWord() throws Exception  {
		try {
			QueryResult res = IndexReader.getInstance().search(null);
		} catch (ApplicationFault ex) {
			assertEquals("Blank Query", ex.getMessage().trim());
		}
	}
	
	public void test2CharacterWord(String id) throws Exception  {
		HDocument doc1 = new HDocument();
		doc1.originalId = "Id : " + id ;
		doc1.title = "Title : " + "The Sun God RA was worshipped by Egyptians.";
		IndexWriter.getInstance().insert(doc1);
		QueryContext ctx = new QueryContext("ra");
		
		QueryResult res = IndexReader.getInstance().search(ctx);
		assertNotNull(res.teasers);
		assertEquals(1, res.teasers.length);
		DocTeaserWeight teaser = (DocTeaserWeight) res.teasers[0];
		assertEquals(new String(teaser.id.toBytes()), doc1.originalId);
		assertEquals(new String(teaser.title.toBytes()), doc1.title);
		
		IndexWriter.getInstance().delete(doc1.originalId);
	}
	
	public void testSpecialCharacter(String id) throws Exception  {
		HDocument doc1 = new HDocument();
		doc1.originalId = "Id : " + id ;
		doc1.title = "For the Sin!1 city I will design wines & wives";
		IndexWriter.getInstance().insert(doc1);

		QueryContext ctx = new QueryContext("wines & wives");
		QueryResult res = IndexReader.getInstance().search(ctx);
		assertNotNull(res.teasers);
		assertEquals(1, res.teasers.length);
		DocTeaserWeight teaser = (DocTeaserWeight) res.teasers[0];
		assertEquals(new String(teaser.id.toBytes()), doc1.originalId);
		assertEquals(new String(teaser.title.toBytes()), doc1.title);
		
		ctx = new QueryContext("Sin!1");
		res = IndexReader.getInstance().search(ctx);
		assertNotNull(res.teasers);
		assertEquals(1, res.teasers.length);
		teaser = (DocTeaserWeight) res.teasers[0];
		assertEquals(new String(teaser.id.toBytes()), doc1.originalId);
		assertEquals(new String(teaser.title.toBytes()), doc1.title);
	}
	
	
	public void testDocumentType(String id) throws Exception  {
		DocumentType dtype = new DocumentType();
		dtype.types.put("employee", (byte) -113);
		dtype.persist();
		
		TermType ttype = new TermType();
		ttype.types.put("empid", (byte) -100);
		ttype.types.put("name", (byte) -99);
		ttype.persist();

		HDocument doc1 = new HDocument();
		doc1.originalId = "Id 1 : " + id ;
		HField fld1 = new HField("empid", "5183");
		HField fld2 = new HField("name", "Abinash Karan");
		doc1.fields = new ArrayList<HField>();
		doc1.fields.add(fld1);
		doc1.fields.add(fld2);
		doc1.docType = "employee";
		IndexWriter.getInstance().insert(doc1);
		
		HDocument doc2 = new HDocument();
		doc2.originalId = "Id 2 : " + id ;
		HField fld3 = new HField("empid", "5184");
		HField fld4 = new HField("name", "Abinash Bagha");
		doc2.fields = new ArrayList<HField>();
		doc2.fields.add(fld3);
		doc2.fields.add(fld4);
		IndexWriter.getInstance().insert(doc2);

		HDocument doc3 = new HDocument();
		doc3.originalId = "Id 3 : " + id ;
		HField fld5 = new HField("empid", "5185");
		HField fld6 = new HField("name", "Abinash Mohanty");
		doc3.fields = new ArrayList<HField>();
		doc3.fields.add(fld5);
		doc3.fields.add(fld6);
		IndexWriter.getInstance().insert(doc3);

		QueryResult res = IndexReader.getInstance().search(new QueryContext("typ:employee abinash"));
		assertNotNull(res.teasers);
		assertEquals(1, res.teasers.length);
		DocTeaserWeight t1 = (DocTeaserWeight) res.teasers[0];
		String id1 = new String( t1.id.toBytes());

		assertTrue( -1 != id1.indexOf(doc1.originalId) );
		
		IndexWriter.getInstance().delete(doc1.originalId);
		IndexWriter.getInstance().delete(doc2.originalId);
		IndexWriter.getInstance().delete(doc3.originalId);
		
		
	}
	
	public void testAbsentDocType(String id) throws Exception  {
		DocumentType dtype = new DocumentType();
		dtype.types.put("toys", (byte) -109);
		dtype.persist();
		
		TermType ttype = new TermType();
		ttype.types.put("creative", (byte) -98);
		ttype.persist();

		HDocument doc1 = new HDocument();
		doc1.originalId = "Id 1 : " + id ;
		HField fld1 = new HField("creative", "dow");
		doc1.fields = new ArrayList<HField>();
		doc1.fields.add(fld1);
		doc1.docType = "toys";
		IndexWriter.getInstance().insert(doc1);
		
		QueryResult res1 = IndexReader.getInstance().search(new QueryContext("typ:toys dow"));
		assertNotNull(res1.teasers);
		assertEquals(1, res1.teasers.length);
		
		try { 
			IndexReader.getInstance().search(new QueryContext("typ:gal dow"));
		} catch (ApplicationFault ex) {
			assertTrue(ex.getMessage().indexOf("unknown") > 0);
		}
	}
	
	public void testTermType(String id) throws Exception  {
		DocumentType dtype = new DocumentType();
		dtype.types.put("molecules", (byte) -108);
		dtype.persist();
		
		TermType ttype = new TermType();
		ttype.types.put("gas", (byte) -97);
		ttype.persist();

		HDocument doc1 = new HDocument();
		doc1.originalId = "Id 1 : " + id ;
		HField fld1 = new HField("gas", "Hydrogen");
		doc1.fields = new ArrayList<HField>();
		doc1.fields.add(fld1);
		doc1.docType = "molecules";
		IndexWriter.getInstance().insert(doc1);

		QueryResult res1 = IndexReader.getInstance().search(new QueryContext("Hydrogen"));
		System.out.println(res1.toString());
		assertNotNull(res1.teasers);
		//assertEquals(1, res1.teasers.length);
		
		HDocument doc2 = new HDocument();
		doc2.originalId = "Id 2 : " + id ;
		doc2.title = "Water is made of hydrogen and oxygen.";
		IndexWriter.getInstance().insert(doc2);
		
		QueryResult res2 = IndexReader.getInstance().search(new QueryContext("gas:Hydrogen"));
		System.out.println("RES2>>>>" + res2.toString());
		assertEquals(1, res2.teasers.length);

		IndexWriter.getInstance().delete(doc2.originalId);
		IndexWriter.getInstance().delete(doc1.originalId);
	}
	
	public void testAbsentTermType(String id) throws Exception  {
		DocumentType dtype = new DocumentType();
		dtype.types.put("stories", (byte) -87);
		dtype.persist();
		
		TermType ttype = new TermType();
		ttype.types.put("character", (byte) -121);
		ttype.persist();

		HDocument doc1 = new HDocument();
		doc1.originalId = "Id 1 : " + id ;
		HField fld1 = new HField("character", "cyndrella");
		doc1.fields = new ArrayList<HField>();
		doc1.fields.add(fld1);
		doc1.docType = "stories";
		IndexWriter.getInstance().insert(doc1);
		
		try { 
			IndexReader.getInstance().search(new QueryContext("girl:cyndrella"));
		} catch (ApplicationFault ex) {
			assertTrue(ex.getMessage().indexOf("unknown") > 0);
		}
	}
	
	public void testDocumentTypeWithTermType(String id) throws Exception  {
		DocumentType dtype = new DocumentType();
		dtype.types.put("molecules", (byte) -108);
		dtype.types.put("fuel", (byte) -109);
		dtype.persist();
		
		TermType ttype = new TermType();
		ttype.types.put("gas", (byte) -97);
		ttype.persist();

		HDocument doc1 = new HDocument();
		doc1.originalId = "Id 1 : " + id ;
		HField fld1 = new HField("gas", "Hydrogen");
		doc1.fields = new ArrayList<HField>();
		doc1.fields.add(fld1);
		doc1.docType = "molecules";
		IndexWriter.getInstance().insert(doc1);

		HDocument doc2 = new HDocument();
		doc2.originalId = "Id 2 : " + id ;
		HField fld2 = new HField("gas", "Hydrogen");
		doc2.fields = new ArrayList<HField>();
		doc2.fields.add(fld2);
		doc2.docType = "fuel";
		IndexWriter.getInstance().insert(doc2);

		QueryResult res1 = IndexReader.getInstance().search(new QueryContext("gas:Hydrogen typ:molecules"));
		System.out.println(res1.toString());
		assertNotNull(res1.teasers);
		assertEquals(1, res1.teasers.length);
		String f1 = new String(((DocTeaserWeight)res1.teasers[0]).id.toBytes());
		assertEquals(doc1.originalId, f1);
		
		QueryResult res2 = IndexReader.getInstance().search(new QueryContext("gas:Hydrogen typ:fuel"));
		System.out.println("RES2>>>>" + res2.toString());
		assertEquals(1, res2.teasers.length);
		String f2 = new String(((DocTeaserWeight)res2.teasers[0]).id.toBytes());
		assertEquals(doc2.originalId, f2);

		QueryResult res3 = IndexReader.getInstance().search(new QueryContext("gas:Hydrogen"));
		System.out.println("RES3>>>>" + res3.toString());
		assertEquals(2, res3.teasers.length);
		String f12 = f1 + f2;
		assertTrue(f12.indexOf(doc1.originalId) != -1 );
		assertTrue(f12.indexOf(doc2.originalId) != -1 );

		IndexWriter.getInstance().delete(doc2.originalId);
		IndexWriter.getInstance().delete(doc1.originalId);
	}
	
	public void testAnd(String id) throws Exception  {
		DocumentType dtype = new DocumentType();
		dtype.types.put("technology", (byte) -108);
		dtype.persist();
		
		TermType ttype = new TermType();
		ttype.types.put("lang", (byte) -97);
		ttype.types.put("os", (byte) -98);
		ttype.types.put("middleware", (byte) -99);
		ttype.persist();

		HDocument doc1 = new HDocument();
		doc1.originalId = "Id 1 : " + id ;
		doc1.fields = new ArrayList<HField>();
		doc1.fields.add(new HField("lang", "Java"));
		doc1.fields.add(new HField("os", "linux"));
		doc1.fields.add(new HField("middleware", "weblogic"));
		doc1.docType = "technology";
		IndexWriter.getInstance().insert(doc1);
		
		HDocument doc2 = new HDocument();
		doc2.originalId = "Id 2 : " + id ;
		doc2.fields = new ArrayList<HField>();
		doc2.fields.add(new HField("lang", "Java"));
		doc2.fields.add(new HField("os", "windows"));
		doc2.fields.add(new HField("middleware", "weblogic"));
		doc2.docType = "technology";
		IndexWriter.getInstance().insert(doc2);
		
		HDocument doc3 = new HDocument();
		doc3.originalId = "Id 2 : " + id ;
		doc3.fields = new ArrayList<HField>();
		doc3.fields.add(new HField("lang", "Java"));
		doc3.fields.add(new HField("os", "linux"));
		doc3.fields.add(new HField("middleware", "hadoop"));
		doc3.docType = "technology";
		IndexWriter.getInstance().insert(doc3);		

		QueryResult res1 = IndexReader.getInstance().search(new QueryContext("+java +linux"));
		System.out.println("RES1>>>>" + res1.toString());
		assertEquals(2, res1.teasers.length);
		String f12 = new String(((DocTeaserWeight)res1.teasers[0]).id.toBytes()) + 
		new String(((DocTeaserWeight)res1.teasers[1]).id.toBytes());
		assertTrue(f12.indexOf(doc1.originalId) != -1 );
		assertTrue(f12.indexOf(doc3.originalId) != -1 );
		
		QueryResult res2 = IndexReader.getInstance().search(new QueryContext("java AND weblogic"));
		System.out.println("RES2>>>>" + res2.toString());
		assertEquals(2, res2.teasers.length);
		f12 = new String(((DocTeaserWeight)res2.teasers[0]).id.toBytes()) + 
		new String(((DocTeaserWeight)res2.teasers[1]).id.toBytes());
		assertTrue(f12.indexOf(doc1.originalId) != -1 );
		assertTrue(f12.indexOf(doc2.originalId) != -1 );

		IndexWriter.getInstance().delete(doc1.originalId);
		IndexWriter.getInstance().delete(doc2.originalId);
		IndexWriter.getInstance().delete(doc3.originalId);
	}
	
	public void testOr(String id) throws Exception  {
		DocumentType dtype = new DocumentType();
		dtype.types.put("fruit", (byte) -108);
		dtype.persist();
		
		TermType ttype = new TermType();
		ttype.types.put("name", (byte) -97);
		ttype.types.put("season", (byte) -98);
		ttype.types.put("price", (byte) -99);
		ttype.persist();

		HDocument doc1 = new HDocument();
		doc1.originalId = "Id 1 : " + id ;
		doc1.fields = new ArrayList<HField>();
		doc1.fields.add(new HField("name", "Mango"));
		doc1.fields.add(new HField("season", "summer"));
		doc1.fields.add(new HField("price", "Rs70/-"));
		doc1.docType = "fruit";
		IndexWriter.getInstance().insert(doc1);
		
		HDocument doc2 = new HDocument();
		doc2.originalId = "Id 2 : " + id ;
		doc2.fields = new ArrayList<HField>();
		doc2.fields.add(new HField("name", "Banana"));
		doc2.fields.add(new HField("season", "Any"));
		doc2.fields.add(new HField("price", "Rs28/-"));
		doc2.docType = "fruit";
		IndexWriter.getInstance().insert(doc2);
		
		HDocument doc3 = new HDocument();
		doc3.originalId = "Id 3 : " + id ;
		doc3.fields = new ArrayList<HField>();
		doc3.fields.add(new HField("name", "Watermelon"));
		doc3.fields.add(new HField("season", "Summer"));
		doc3.fields.add(new HField("price", "Rs70/-"));
		doc3.docType = "technology";
		IndexWriter.getInstance().insert(doc3);		

		QueryResult res1 = IndexReader.getInstance().search(new QueryContext("Summer Banana"));
		System.out.println("RES1>>>>" + res1.toString());
		assertEquals(3, res1.teasers.length);
		String f123 = new String(((DocTeaserWeight)res1.teasers[0]).id.toBytes()) + 
		new String(((DocTeaserWeight)res1.teasers[1]).id.toBytes())  + 
		new String(((DocTeaserWeight)res1.teasers[2]).id.toBytes());
		
		assertTrue(f123.indexOf(doc1.originalId) != -1 );
		assertTrue(f123.indexOf(doc2.originalId) != -1 );
		assertTrue(f123.indexOf(doc3.originalId) != -1 );
		
		QueryResult res2 = IndexReader.getInstance().search(new QueryContext("summer OR Rs70/-"));
		System.out.println("RES2>>>>" + res2.toString());
		assertEquals(2, res2.teasers.length);
		String f12 = new String(((DocTeaserWeight)res2.teasers[0]).id.toBytes()) + 
		new String(((DocTeaserWeight)res2.teasers[1]).id.toBytes());
		assertTrue(f12.indexOf(doc1.originalId) != -1 );
		assertTrue(f12.indexOf(doc3.originalId) != -1 );

		IndexWriter.getInstance().delete(doc1.originalId);
		IndexWriter.getInstance().delete(doc2.originalId);
		IndexWriter.getInstance().delete(doc3.originalId);		
	}
	
	public void testMultiphrase() throws Exception  {
		HDocument doc1 = new HDocument();
		doc1.originalId = "2312";
		doc1.title = "I born at Keonjhar Orissa";
		IndexWriter.getInstance().insert(doc1);
		QueryResult res1 = IndexReader.getInstance().search(new QueryContext("Keonjhar Orissa"));
		assertTrue(new String(((DocTeaserWeight)res1.teasers[0]).id.toBytes()).indexOf(doc1.originalId) != -1 );

		IndexWriter.getInstance().delete(doc1.originalId);
	}
	
	public void testQuotedMultiphrase() throws Exception  {
		HDocument doc1 = new HDocument();
		doc1.originalId = "2313";
		doc1.title = "Oriya is my mother toungh. I do lot of spelling mistakes in english.";
		IndexWriter.getInstance().insert(doc1);
		QueryResult res1 = IndexReader.getInstance().search(new QueryContext("\"mother toungh\""));
		assertTrue(new String(((DocTeaserWeight)res1.teasers[0]).id.toBytes()).indexOf(doc1.originalId) != -1 );

		IndexWriter.getInstance().delete(doc1.originalId);
	}
	
	public void testStopword() throws Exception  {
		HDocument doc1 = new HDocument();
		doc1.originalId = "2314";
		List<String> stopwords = new ArrayList<String> ();
		stopwords.add("a");
		stopwords.add("and");
		StopwordManager.getInstance().setStopwords(stopwords);
		new StopwordRefresh().process(); //A 30mins job is done in sync mode

		doc1.title = "Once upon a time a tiger and a horse were staying together.";
		IndexWriter.getInstance().insert(doc1);
		try {
			IndexReader.getInstance().search(new QueryContext("and"));
		} catch (Exception ex) {
			assertTrue(ex.getMessage().indexOf("Word not Recognized") >= 0);
		}
		
		IndexWriter.getInstance().delete( "2314");
	}
	
	public void testMultiphraseWhereOneIsStopWord() throws Exception  {
		HDocument doc1 = new HDocument();
		doc1.originalId = "2314";
		List<String> stopwords = new ArrayList<String> ();
		stopwords.add("a");
		stopwords.add("and");
		StopwordManager.getInstance().setStopwords(stopwords);
		new StopwordRefresh().process(); //A 30mins job is done in sync mode

		doc1.title = "Once upon a time a tiger and a crocodile were staying together.";
		IndexWriter.getInstance().insert(doc1);
		QueryResult res = IndexReader.getInstance().search(new QueryContext("and a crocodile"));
		assertNotNull(res);
		assertEquals(1, res.teasers.length);
		
		IndexWriter.getInstance().delete( "2314");
	}
	
	public void testTypeAndNonTypeMixed(String id) throws Exception  {
		DocumentType dtype = new DocumentType();
		dtype.types.put("fruit", (byte) -108);
		dtype.persist();
		
		TermType ttype = new TermType();
		ttype.types.put("name", (byte) -97);
		ttype.types.put("price", (byte) -99);
		ttype.persist();

		HDocument doc1 = new HDocument();
		doc1.originalId = "Id 1 : " + id ;
		doc1.fields = new ArrayList<HField>();
		doc1.fields.add(new HField("name", "apple"));
		doc1.fields.add(new HField("price", "123.00"));
		doc1.docType = "fruit";
		IndexWriter.getInstance().insert(doc1);

		QueryResult res = IndexReader.getInstance().search(new QueryContext("+name:apple +123.00"));
		assertNotNull(res);
		assertEquals(1, res.teasers.length);
		String f1 = new String(((DocTeaserWeight)res.teasers[0]).id.toBytes());
		assertTrue(f1.indexOf(doc1.originalId) != -1 );

		IndexWriter.getInstance().delete(doc1.originalId);
	}
	
	public void test_TypeNonType_WrongType_NonExistance(String id) throws Exception  {
		DocumentType dtype = new DocumentType();
		dtype.types.put("baby", (byte) -108);
		dtype.persist();
		
		TermType ttype = new TermType();
		ttype.types.put("babyname", (byte) -97);
		ttype.types.put("house", (byte) -99);
		ttype.persist();

		HDocument doc1 = new HDocument();
		doc1.originalId = "Id 1 : " + id ;
		doc1.fields = new ArrayList<HField>();
		doc1.fields.add(new HField("babyname", "Ava"));
		doc1.fields.add(new HField("house", "466"));
		doc1.docType = "baby";
		IndexWriter.getInstance().insert(doc1);

		try {
			IndexReader.getInstance().search(new QueryContext(
			"+XX:Ava +466"));
		} catch (Exception ex) {
			assertTrue(ex.getMessage().indexOf("unknown") >= 0);
		}
		

		QueryResult res3 = IndexReader.getInstance().search(new QueryContext(
		"+babyname:Ava 466"));
		assertEquals(1, res3.teasers.length);

		QueryResult res1 = IndexReader.getInstance().search(new QueryContext(
		"+babyname:Ava +jhumritalya"));
		assertEquals(0, res1.teasers.length);
		
		IndexWriter.getInstance().delete(doc1.originalId);
	}

	public void testDocumentStateFilter(String id) throws Exception  {
		DocumentType dtype = new DocumentType();
		dtype.types.put("leave", (byte) -108);
		dtype.persist();
		
		TermType ttype = new TermType();
		ttype.types.put("fromdate", (byte) -97);
		ttype.persist();

		HDocument doc1 = new HDocument();
		doc1.originalId = "Id 1 : " + id ;
		doc1.fields = new ArrayList<HField>();
		doc1.fields.add(new HField("fromdate", "12thDec,2009"));
		doc1.docType = "leave";
		doc1.state =  "closed";
		IndexWriter.getInstance().insert(doc1);

		QueryResult res1 = IndexReader.getInstance().search(
			new QueryContext("12thDec,2009"));
		assertEquals(1, res1.teasers.length);
		
		QueryResult res2 = IndexReader.getInstance().search(
				new QueryContext("ste:_active 12thDec,2009"));
			assertEquals(0, res2.teasers.length);

		IndexWriter.getInstance().delete(doc1.originalId);
		
	}
	
	public void testTenant(String id) throws Exception  {

		HDocument doc1 = new HDocument();
		doc1.originalId = "Id 1 : " + id ;
		doc1.title = "contactus In Call center";
		doc1.tenant = "icici";
		IndexWriter.getInstance().insert(doc1);
		
		HDocument doc2 = new HDocument();
		doc2.originalId = "Id 2 : " + id ;
		doc2.title = "contactus Offshore";
		doc2.docType = "leave";
		doc2.tenant = "infosys";
		IndexWriter.getInstance().insert(doc2);

		QueryResult res1 = IndexReader.getInstance().search(
				new QueryContext("tenant:icici contactus"));
		assertEquals(1, res1.teasers.length);
		String f1 = new String(((DocTeaserWeight)res1.teasers[0]).id.toBytes());
		assertEquals(doc1.originalId, f1);
			
		QueryResult res2 = IndexReader.getInstance().search(
				new QueryContext("contactus"));
		assertEquals(2, res2.teasers.length);

		IndexWriter.getInstance().delete(doc1.originalId);
		IndexWriter.getInstance().delete(doc2.originalId);
		
	}
	
	public void testCreatedBefore(String id) throws Exception  {

		HDocument doc1 = new HDocument();
		doc1.originalId = "Id 1 : " + id ;
		doc1.title = "Born at poland around 1800 century past.";
		doc1.bornOn = new Date(System.currentTimeMillis()-240000000);
		IndexWriter.getInstance().insert(doc1);

		Date date = new Date();
		String longDate = new Long(date.getTime()).toString();
		String query = "createdb:" + longDate + " century";

		QueryResult res = IndexReader.getInstance().search(
				new QueryContext(query));
		assertEquals(1, res.teasers.length);
		
		String pastDate = new Long(doc1.bornOn.getTime() - 300000000).toString();
		String pastQ = "createdb:" + pastDate + " century";
		QueryResult pastRes = IndexReader.getInstance().search(
				new QueryContext(pastQ));
		assertEquals(0, pastRes.teasers.length);

		IndexWriter.getInstance().delete(doc1.originalId);
	}

	public void testCreatedAfter() throws Exception  {
		Date date = new Date();
		String longDate = new Long(date.getTime()).toString();
		Thread.sleep(2000);
		
		String humanoid1 = "Humanoid 1 came to earth in 2050 searchcherry future.";
		String query = "borna:" + longDate + " searchcherry";
	}
	
	public void testModifiedAfter() throws Exception  {
		String content1 = "Now the touch me not is opened";
		
		Date date = new Date();
		String longDate = new Long(date.getTime()).toString();
		Thread.sleep(2000);
		
		String content2 = "Now the touch me not is closed";
		String query1 = "modifieda:" + longDate + " touch";

	}
	
	public void testModifiedBefore() throws Exception  {
		String content1 = "Now the touch me not is opened";
		
		Date date = new Date();
		String longDate = new Long(date.getTime()).toString();
		Thread.sleep(2000);
		
		String content2 = "Now the touch me not is closed";
		String query1 = "modifiedb:" + longDate + " touch";
	}
	
	public void testBodyFetchLimit() throws Exception  {
		String query1 = "dfl:3 abinash";
		String query2 = "dfl:1 abinash";
		String query3 = "dfl:0 abinash";
		String query4 = "dfl:-1 abinash";
	}
	
	public void testMetaFetchLimit() throws Exception  {
		String query1 = "mfl:3 abinash";
	}
	
	public void testTeaserLength() throws Exception  {
		String query1 = "tsl:30 dfl:1000 abinash";
	}
	
	public void testDataBackupIntegrity() throws Exception  {
		
	}
	
	public void testAccessAllow() throws Exception  {
		
	}

	public void testAccessDeny() throws Exception {
		
	}

	public void testNonLoggedInIndexCreation() throws Exception {
		
	}

	public void testAccessAnonymous() throws Exception  {
		
	}

}
