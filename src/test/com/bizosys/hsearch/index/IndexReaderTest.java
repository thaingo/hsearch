package com.bizosys.hsearch.index;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import junit.framework.TestCase;

import com.bizosys.hsearch.common.HDocument;
import com.bizosys.hsearch.common.HField;
import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.dictionary.DictionaryManager;
import com.bizosys.hsearch.query.DocTeaserWeight;
import com.bizosys.hsearch.query.DocWeight;
import com.bizosys.hsearch.query.QueryContext;
import com.bizosys.hsearch.query.QueryResult;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.services.ServiceFactory;


public class IndexReaderTest extends TestCase {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		ServiceFactory.getInstance().init(conf, null);
		
		IndexReaderTest t = new IndexReaderTest();
        //TestFerrari.testAll(t);
		t.testTermType("ID0213213");
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
		assertEquals(3, res.teasers.length);
		DocTeaserWeight t1 = (DocTeaserWeight) res.teasers[0];
		String id1 = new String( t1.id.toBytes());

		DocTeaserWeight t2 = (DocTeaserWeight) res.teasers[1];
		String id2 = new String( t2.id.toBytes());
		
		DocTeaserWeight t3 = (DocTeaserWeight) res.teasers[2];
		String id3 = new String( t3.id.toBytes());
		
		String allIds = id1 + " " + id2 + " " + id3;

		assertTrue( -1 != allIds.indexOf(doc1.originalId) );
		assertTrue( -1 != allIds.indexOf(doc2.originalId) );
		assertTrue( -1 != allIds.indexOf(doc3.originalId) );
		
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
		
		QueryResult res2 = IndexReader.getInstance().search(new QueryContext("typ:gal dow"));
		assertEquals(0, res2.teasers.length);
		IndexWriter.getInstance().delete(doc1.originalId);
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
		System.out.println(res2.teasers.toString());
		assertEquals(1, res2.teasers.length);

		IndexWriter.getInstance().delete(doc2.originalId);
		IndexWriter.getInstance().delete(doc1.originalId);
	}
	
	public void testAbsentTermType() throws Exception  {
		String content = "<stories><character>cyndrella</character></stories>";
		String query1 = "girl:cyndrella";
	}
	
	public void testDocumentTypeWithTermType() throws Exception  {
		String content1 = "<molecules><gas>Hydrogen</gas></molecules>";
		String content2 = "<fuel><gas>Hydrogen</gas></fuel>";
		
		String query1 = "gas:Hydrogen typ:molecules";
		String query2 = "gas:Hydrogen typ:fuel";
		String query3 = "gas:Hydrogen";
	}
	
	public void testAnd() throws Exception  {
		String content1 = "<technology><lang>Java</lang><os>linux</os><middleware>weblogic</middleware></technology>";
		String content2 = "<technology><lang>Java</lang><os>windows</os><middleware>weblogic</middleware></technology>";
		String content3 = "<technology><lang>Java</lang><os>linux</os><middleware>hadoop</middleware></technology>";
		
		String query1 = "+java +linux";
		String query2 = "java AND weblogic";
	}
	
	public void testOr() throws Exception  {
		String content1 = "<technology><lang>Java</lang><os>linux</os><middleware>weblogic</middleware></technology>";
		String content2 = "<technology><lang>Java</lang><os>linux</os><middleware>hadoop</middleware></technology>";
		String content3 = "<technology><lang>Java</lang><os>windows</os><middleware>weblogic</middleware></technology>";
		
		String query1 = "java linux";
		String query2 = "hadoop OR Linux";
	}
	
	public void testMultiphrase() throws Exception  {
		String content = "I born at Keonjhar Orissa";
		String query = "Keonjhar Orissa";
	}
	
	public void testQuotedMultiphrase() throws Exception  {
		String content = "Oriya is my mother toungh. I do lot of spelling mistakes in english.";
		String query = "\"mother toungh\"";
	}
	
	public void testStopword() throws Exception  {
		String content = "Once upon a time a tiger and a horse were staying together.";
		String query = "and";
	}
	
	public void testMultiphraseWhereOneIsStopWord() throws Exception  {
		String content = "Once upon a time a tiger and a http were staying together.";
		String query = "http";
	}
	
	public void testMultiphraseWithType() throws Exception  {
		String content = "<citizens><name>abinash karan</name></citizens>";
		String query = "+name:abinash_karan";
	}
	
	public void testTypeAndNonTypeMixed() throws Exception  {
		String content = "<fruits><name>apple</name><price>123.00</price></fruits>";
		String query = "+name:apple +123.00";
	}
	
	public void testWrongTypeAndNonTypeMixed() throws Exception  {
		String content = "<baby><babyname>Ava</babyname><house>466</house></baby>";
		String query = "+XX:Ava +466";
	}
	public void testTypeAndNonExistanceWordMixed() throws Exception  {
		String content = "<baby><babyname>Ava</babyname><house>466</house></baby>";
		String query = "+babyname:Ava +jhumritalya";
	}

	public void testFieldNameSerch() throws Exception  {
		String content = "<state><district>Keonjhar</district><district>Mayurbhanj</district></state>";
		String content2 = "<state><district>Cuttack</district><district>Puri</district></state>";
		String query = "district";
	}
	
	public void testDocumentStateFilter() throws Exception  {
		String content = "<leave><fromdate>12thDec,2009</fromdate></leave>";
		DocMeta meta1 = new DocMeta();
		meta1.state = "active";

		DocMeta meta2 = new DocMeta();
		meta2.state = "inactive";

		String query1 = "ste:_active 12thDec,2009";
	}
	
	public void testTenant() throws Exception  {
		String content1 = "contactus Icici";
		DocMeta meta1 = new DocMeta();
		meta1.tenant = "icici";
		String query1 = "tenant:icici contactus";
		
		String content2 = "contactus Infosys";
		DocMeta meta2 = new DocMeta();
		meta2.tenant = "infosys";
		String query2 = "tenant:Infosys contactus";
		
		String query3 = "ou: contactus";
		
	}
	
	public void testCreatedBefore() throws Exception  {
		String content = "Born at poland around 1800 century past.";
		Date date = new Date();
		String longDate = new Long(date.getTime()).toString();
		Thread.sleep(2000);
		
		String query = "bornb:" + longDate + " century";
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
	
	public void testMetaFieldFilters() throws Exception  {
		/**
		DocMeta coldMeta = new DocMeta();
		coldMeta.createdOn = HttpDateFormat.toDate("Sat, 05 Feb 1977 15:35:00 IST");
		coldMeta.validTill = HttpDateFormat.toDate("Wed, 11 Dec 2047 16:45:00 IST");
		coldMeta.eastering = 44.24f;
		coldMeta.geoHouse = "42 A";
		coldMeta.ipHouse = 99999999;
		coldMeta.northing = 73.17f;
		coldMeta.sentimentPositive = true;
		coldMeta.securityHigh = false;
		coldMeta.state = "active";
		coldMeta.tenant = "bizosys";
		coldMeta.modifiedOn = HttpDateFormat.toDate("Fri, 11 Dec 2009 15:35:00 IST");
		coldMeta.docType = "person";
		coldMeta.weight = 65;

		assertEquals("house218619 type0", hotMeta.owner.toString());
		assertEquals(HttpDateFormat.toDate("Sat, 05 Feb 1977 15:35:00 IST").getTime(), hotMeta.born.getTime());
		assertEquals(HttpDateFormat.toDate("Wed, 11 Dec 2047 16:45:00 IST").getTime(), hotMeta.death.getTime());
		assertEquals(44.24f, hotMeta.eastering);
		assertEquals("21904234", hotMeta.folderName);
		assertEquals("42 A", hotMeta.geoHouse);
		assertEquals("101", hotMeta.id);
		assertEquals(99999999, hotMeta.ipHouse);
		assertEquals(73.17f, hotMeta.northing);
		assertEquals(true, hotMeta.sentiment);
		assertEquals(DocMeta.SECURITY_MEDIUM, hotMeta.securityLevel);
		assertTrue(hotMeta.sizeInKb == 165);
		assertEquals("active", hotMeta.state);
		assertEquals("searchcherry", hotMeta.orgUnit);
		assertEquals(HttpDateFormat.toDate("Fri, 11 Dec 2009 15:35:00 IST").getTime(), hotMeta.touch.getTime());
		assertEquals("person", hotMeta.type);
		assertEquals(65, hotMeta.weight);
		*/
		
	}
	
	public void testScoreBoosterOnDocWeight() throws Exception  {
		
	}
	
	public void testScoreBoosterOnIPProximity() throws Exception  {
		
	}
	
	public void testScoreBoosterOnAuthorProximity() throws Exception  {
		
	}

	public void testScoreBoosterOnFreshness() throws Exception  {
		
	}

	public void testScoreBoosterOnSocialRanking() throws Exception  {
		
	}
	
	public void testScoreBoosterOnPreciousness() throws Exception  {
		
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
