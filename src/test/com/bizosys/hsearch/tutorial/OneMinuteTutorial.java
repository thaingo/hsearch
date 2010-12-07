/*
* http://www.apache.org/licenses/LICENSE-2.0
*/
package com.bizosys.hsearch.tutorial;

import junit.framework.TestCase;

import com.bizosys.hsearch.common.HDocument;
import com.bizosys.hsearch.index.DocTeaser;
import com.bizosys.hsearch.index.IndexReader;
import com.bizosys.hsearch.index.IndexWriter;
import com.bizosys.hsearch.query.QueryContext;
import com.bizosys.hsearch.query.QueryResult;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.services.ServiceFactory;


public class OneMinuteTutorial extends TestCase {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(); //Load configuration file (site.xml)
		ServiceFactory.getInstance().init(conf, null); //Initialize all services
		OneMinuteTutorial.testHello("BIZOSYS-001", "Welcome to hello world", "hello"); //Run Helloworld
	}

	public static void testHello(String id, String title, String query) throws Exception  {
		IndexWriter.getInstance().insert(new HDocument(id, title)); //Index a HDocument
		QueryContext ctx = new QueryContext(query); //Form a query context
		QueryResult res = IndexReader.getInstance().search(ctx); //Perform Search
		assertEquals(1, res.teasers.length); //Matching records count
		DocTeaser firstRecord = (DocTeaser) res.teasers[0]; //Get first record
		System.out.println(firstRecord.getId() + "\n" + firstRecord.title.toString());
	}
}
