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
package com.bizosys.hsearch.index;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import junit.framework.TestCase;

import com.bizosys.hsearch.common.HDocument;
import com.bizosys.hsearch.common.HField;
import com.bizosys.hsearch.filter.Access;
import com.bizosys.hsearch.inpipe.util.StopwordManager;
import com.bizosys.hsearch.inpipe.util.StopwordRefresh;
import com.bizosys.hsearch.query.DocTeaserWeight;
import com.bizosys.hsearch.query.QueryContext;
import com.bizosys.hsearch.query.QueryResult;
import com.bizosys.hsearch.security.WhoAmI;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.services.ServiceFactory;


public class IndexReaderTest extends TestCase {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		ServiceFactory.getInstance().init(conf, null);
		
		IndexReaderTest t = new IndexReaderTest();
        //TestFerrari.testRandom(t);
		t.testGuest("SPecial-1");
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
			IndexReader.getInstance().search(null);
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
		assertTrue(res.teasers.length > 0);
		String allIds = "";
		for ( Object stwO : res.teasers) {
			teaser = (DocTeaserWeight) stwO;
			allIds =  new String(teaser.id.toBytes()) + allIds; 
		}
		System.out.println(allIds);
		assertTrue(allIds.indexOf(doc1.originalId) >= 0);
		IndexWriter.getInstance().delete(doc1.originalId);
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
		doc1.createdOn = new Date(System.currentTimeMillis()-240000000);
		IndexWriter.getInstance().insert(doc1);

		Date date = new Date();
		String longDate = new Long(date.getTime()).toString();
		String query = "createdb:" + longDate + " century";

		QueryResult res = IndexReader.getInstance().search(
				new QueryContext(query));
		assertEquals(1, res.teasers.length);
		
		String pastDate = new Long(doc1.createdOn.getTime() - 300000000).toString();
		String pastQ = "createdb:" + pastDate + " century";
		QueryResult pastRes = IndexReader.getInstance().search(
				new QueryContext(pastQ));
		assertEquals(0, pastRes.teasers.length);

		IndexWriter.getInstance().delete(doc1.originalId);
	}

	public void testCreatedAfter(String id) throws Exception  {

		DateFormat format = DateFormat.getDateTimeInstance(
            DateFormat.MEDIUM, DateFormat.SHORT);
		
		HDocument doc1 = new HDocument();
		doc1.originalId = "Id 1 : " + id ;
		doc1.title = "My daughter birth was after my birth";
		doc1.createdOn = format.parse("Nov 18, 2008 6:15 AM");
		IndexWriter.getInstance().insert(doc1);

		String myBirth = new Long(format.parse("Feb 05, 1977 8:00 PM").getTime()).toString();
		QueryResult res1 = IndexReader.getInstance().search(
				new QueryContext("createda:" + myBirth + " birth"));
		assertEquals(1, res1.teasers.length);
		
		String toDate = new Long(new Date().getTime()).toString();
		QueryResult res2 = IndexReader.getInstance().search(
				new QueryContext("createda:" + toDate + " birth"));
		assertEquals(0, res2.teasers.length);

		IndexWriter.getInstance().delete(doc1.originalId);
	}
	
	public void testModifiedAfter(String id) throws Exception  {
		DateFormat format = DateFormat.getDateTimeInstance(
	            DateFormat.MEDIUM, DateFormat.SHORT);

		HDocument doc1 = new HDocument();
		doc1.originalId = "Id 1 : " + id ;
		doc1.title = "My Trading balance as 234.00";
		doc1.modifiedOn = new Date();
		IndexWriter.getInstance().insert(doc1);

		String myBirth = new Long(format.parse("Feb 05, 1977 8:00 PM").getTime()).toString();
		QueryResult res1 = IndexReader.getInstance().search(
				new QueryContext("modifieda:" + myBirth + " balance"));
		assertEquals(1, res1.teasers.length);
			
		String future = new Long(format.parse("Feb 05, 2121 8:00 PM").getTime()).toString();
		QueryResult res2 = IndexReader.getInstance().search(
				new QueryContext("modifieda:" + future + " balance"));
		assertEquals(0, res2.teasers.length);

		IndexWriter.getInstance().delete(doc1.originalId);
	}
	
	public void testModifiedBefore(String id) throws Exception  {
		DateFormat format = DateFormat.getDateTimeInstance(
	            DateFormat.MEDIUM, DateFormat.SHORT);

		HDocument doc1 = new HDocument();
		doc1.originalId = "Id 1 : " + id ;
		doc1.title = "My Trading balance as 234.00";
		doc1.modifiedOn = new Date();
		IndexWriter.getInstance().insert(doc1);

		String myBirth = new Long(format.parse("Feb 05, 1977 8:00 PM").getTime()).toString();
		QueryResult res1 = IndexReader.getInstance().search(
				new QueryContext("modifiedb:" + myBirth + " balance"));
		assertEquals(0, res1.teasers.length);
			
		String future = new Long(format.parse("Feb 05, 2121 8:00 PM").getTime()).toString();
		QueryResult res2 = IndexReader.getInstance().search(
				new QueryContext("modifiedb:" + future + " balance"));
		assertEquals(1, res2.teasers.length);

		IndexWriter.getInstance().delete(doc1.originalId);
	}
	
	public void testDocumentFetchLimit(String id) throws Exception  {
		ArrayList<HDocument> docs = new ArrayList<HDocument>();
		for ( int i=0; i<10; i++) {
			HDocument doc = new HDocument();
			doc.originalId = i + " - " + id ;
			doc.title = "Flower " + i;
			docs.add(doc);
		}
		IndexWriter.getInstance().insert(docs);

		QueryResult res1 = IndexReader.getInstance().search(
				new QueryContext("dfl:3 flower"));
		assertEquals(3, res1.teasers.length);

		QueryResult res2 = IndexReader.getInstance().search(
				new QueryContext("dfl:1 flower"));
		assertEquals(1, res2.teasers.length);
		
		QueryResult res3 = IndexReader.getInstance().search(
				new QueryContext("dfl:0 flower"));
		assertEquals(0, res3.teasers.length);
		
		for ( int i=0; i<10; i++) {
			IndexWriter.getInstance().delete(i + " - " + id);
		}
	}
	
	public void testMetaFetchLimit(String id) throws Exception  {
		ArrayList<HDocument> docs = new ArrayList<HDocument>();
		for ( int i=0; i<10; i++) {
			HDocument doc = new HDocument();
			doc.originalId = i + " - " + id ;
			doc.title = "Flower " + i;
			docs.add(doc);
		}
		IndexWriter.getInstance().insert(docs);

		QueryResult res1 = IndexReader.getInstance().search(
				new QueryContext("mfl:3 flower"));
		assertEquals(3, res1.teasers.length);

		QueryResult res2 = IndexReader.getInstance().search(
				new QueryContext("mfl:1 flower"));
		assertEquals(1, res2.teasers.length);
		
		QueryResult res3 = IndexReader.getInstance().search(
				new QueryContext("mfl:0 flower"));
		assertEquals(0, res3.teasers.length);
		
		for ( int i=0; i<10; i++) {
			IndexWriter.getInstance().delete(i + " - " + id);
		}
	}
	
	public void testTeaserLength(String id) throws Exception  {
		HDocument doc = new HDocument();
		doc.originalId = id;
		doc.title = "Using SimpleDateFormat for custom date formatting and parsing";
		doc.cacheText = "The default DateFormat instances returned by the static methods in the DateFormat class may be sufficient for many purposes, but clearly do not cover all possible valid or useful formats for dates. For example, notice that in Figure 2, none of the DateFormat-generated strings (numbers 2 - 9) match the format of the output of the Date class’s toString() method. This means that you cannot use the default DateFormat instances to parse the output of toString(), something that might be useful for things like parsing log data.The SimpleDateFormat lets you build custom formats. Dates are constructed with a string that specifies a pattern for the dates to be formatted and/or parsed. From the SimpleDateFormat JavaDocs, the characters in Figure 7 can be used in date formats. Where appropriate, 4 or more of the character will be interpreted to mean that the long format of the element should be used, while fewer than 4 mean that a short format should be used.";
		IndexWriter.getInstance().insert(doc);
		QueryResult middleCut = IndexReader.getInstance().search(
				new QueryContext("tsl:30 purposes"));
		assertEquals(1, middleCut.teasers.length);
		DocTeaserWeight dtw = (DocTeaserWeight) middleCut.teasers[0]; 
		assertEquals(30, dtw.cacheText.toBytes().length);

		QueryResult frontCut = IndexReader.getInstance().search(
				new QueryContext("tsl:100 DateFormat"));
		assertEquals(1, frontCut.teasers.length);
		dtw = (DocTeaserWeight) frontCut.teasers[0]; 
		assertEquals(100, dtw.cacheText.toBytes().length);
		
		QueryResult endCut = IndexReader.getInstance().search(
				new QueryContext("tsl:100 used"));
		assertEquals(1, endCut.teasers.length);
		dtw = (DocTeaserWeight) endCut.teasers[0]; 
		assertEquals(100, dtw.cacheText.toBytes().length);

		IndexWriter.getInstance().delete(doc.originalId);
	}
	
	public void testAccessAllow(String id) throws Exception  {
		HDocument doc = new HDocument();
		doc.originalId = id;
		doc.title = "Welcome to IIT Library";
		Access viewPerm = new Access();
		viewPerm.addOrgUnit("bizosys");
		doc.viewPermission = viewPerm; 
		IndexWriter.getInstance().insert(doc);
		
		WhoAmI whoami = new WhoAmI();
		whoami.ou = "bizosys";
		whoami.uid = "n-4501";
		
		QueryContext ctx = new QueryContext("IIT");
		ctx.user = whoami;
		QueryResult res = IndexReader.getInstance().search(ctx); 
		assertEquals(1, res.teasers.length);

		IndexWriter.getInstance().delete(doc.originalId);
	}

	public void testAccessDeny(String id) throws Exception {
		HDocument doc = new HDocument();
		doc.originalId = id;
		doc.title = "Welcome to IIT Library";
		Access viewPerm = new Access();
		viewPerm.addOrgUnit("bizosys");
		doc.viewPermission = viewPerm; 
		IndexWriter.getInstance().insert(doc);
		
		WhoAmI whoami = new WhoAmI();
		whoami.ou = "infosys";
		whoami.uid = "n-4501";
		
		QueryContext ctx = new QueryContext("IIT");
		ctx.user = whoami;
		QueryResult res = IndexReader.getInstance().search(ctx); 
		assertEquals(0, res.teasers.length);

		IndexWriter.getInstance().delete(doc.originalId);
	}

	public void testGuest(String id) throws Exception {
		HDocument doc = new HDocument();
		doc.originalId = id;
		doc.title = "Register for private blogging at VOX.";
		Access viewPerm = new Access();
		viewPerm.addOrgUnit("bizosys");
		doc.viewPermission = viewPerm; 
		IndexWriter.getInstance().insert(doc);
		
		QueryContext ctx = new QueryContext("VOX");
		QueryResult res = IndexReader.getInstance().search(ctx); 
		assertEquals(0, res.teasers.length);

		IndexWriter.getInstance().delete(doc.originalId);		
	}

	public void testAccessAnonymous(String id) throws Exception  {
		HDocument doc = new HDocument();
		doc.originalId = id;
		doc.title = "Manmohan Singh is prime minister of India";
		Access viewPerm = new Access();
		viewPerm.addAnonymous();
		doc.viewPermission = viewPerm; 
		IndexWriter.getInstance().insert(doc);
		
		QueryContext ctx = new QueryContext("Singh");
		QueryResult res = IndexReader.getInstance().search(ctx); 
		assertEquals(1, res.teasers.length);

		IndexWriter.getInstance().delete(doc.originalId);		
	}
}
