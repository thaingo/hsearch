package com.bizosys.hsearch.index;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import junit.framework.TestCase;

import com.bizosys.hsearch.common.HDocument;
import com.bizosys.hsearch.common.HField;
import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.query.QueryContext;
import com.bizosys.hsearch.query.QueryResult;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.services.ServiceFactory;


public class IndexReaderTest extends TestCase {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		ServiceFactory.getInstance().init(conf, null);
		
		IndexReaderTest t = new IndexReaderTest();
        //TestFerrari.testAll(t);
		t.testGetAll();
	}

	public void testGet() throws Exception {
		String content = "Welcome to bizosys technologies";
		
		HDocument doc1 = new HDocument();
		doc1.originalId = new Storable("BIZOSYS-001 ");
		doc1.title = new Storable("Test Vanila");
		doc1.fields = new ArrayList<HField>();
		HField fld = new HField("BODY", content);
		doc1.fields.add(fld);
		List<HDocument> docs = new ArrayList<HDocument>();
		docs.add(doc1);
		IndexWriter.getInstance().insert(docs);
		
		Doc d = IndexReader.getInstance().get("BIZOSYS-001");
		assertNotNull(d);
		System.out.println(d.toString());
	}
	
	public void testGetAll() throws Exception {
		String content = "How is life at Keonjhar, Orissa";
		
		HDocument doc1 = new HDocument();
		doc1.originalId = new Storable("BIZOSYS-001");
		doc1.title = new Storable("Keonjhar, Jagannathpur");
		doc1.fields = new ArrayList<HField>();
		HField fld = new HField("BODY", content);
		doc1.fields.add(fld);
		List<HDocument> docs = new ArrayList<HDocument>();
		docs.add(doc1);
		IndexWriter.getInstance().insert(docs);
		Doc doc = IndexReader.getInstance().get("BIZOSYS-002");
		List<InvertedIndex> iiL = 
			IndexReader.getInstance().getInvertedIndex(doc.bucketId);
		for ( InvertedIndex ii : iiL) System.out.println(ii.toString());
	}
	
	
	public void testVanilla() throws Exception  {
		String content = "Welcome to bizosys technologies";
		
		HDocument doc1 = new HDocument();
		doc1.originalId = new Storable("BIZOSYS-001");
		doc1.title = new Storable("Test Vanila");
		doc1.fields = new ArrayList<HField>();
		HField fld = new HField("BODY", content);
		doc1.fields.add(fld);
		List<HDocument> docs = new ArrayList<HDocument>();
		docs.add(doc1);
		
		IndexWriter.getInstance().insert(docs);

		QueryContext ctx = new QueryContext("bizosys");
		QueryResult res = IndexReader.getInstance().search(ctx);
		System.out.println(res.toString());
	}

	public void testNullWord() throws Exception  {
		String query = null;
	}
	
	public void test2CharacterWord() throws Exception  {
		String content = "The Sun God RA was worshipped by Egyptians.";
		String query = "RA";
	}
	
	public void testSpecialCharacter() throws Exception  {
		String content = "For the Sin!1 city I will design wines & wives";
		String query1 = "wines & wives";
		String query2 = "Sin!1";
	}
	
	
	public void testDocumentType() throws Exception  {
		String content1 = "<employee><empid>5183</empid><name>Abinash Karan</name></employee>";
		String content2 = "<employee><empid>5184</empid><name>Abinash Karan</name></employee>";
		String content3 = "<employee><empid>5185</empid><name>Abinash Karan</name></employee>";
		
		String query = "typ:employee abinash";
	}
	
	public void testAbsentDocType() throws Exception  {
		String content = "<toys><creative>dow</creative></toys>";
		String query1 = "typ:toys dow";
		String query2 = "typ:gal dow";
		
	}
	
	public void testTermType() throws Exception  {
		String content1 = "<molecules><gas>Hydrogen</gas></molecules>";
		String content2 = "Water is made of hydrogen and oxygen.";
		String query1 = "Hydrogen";
		String query2 = "gas:Hydrogen";
		
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
