package com.bizosys.hsearch.index;

import java.util.Date;

import junit.framework.TestCase;

import com.bizosys.ferrari.TestFerrari;

public class DocTeaserTest extends TestCase {

	public static void main(String[] args) throws Exception {
		DocTeaserTest t = new DocTeaserTest();
        TestFerrari.testAll(t);
	}
	
	public void testSerialize(String type, String state, 
		String orgunit, String geohouse, Long created, Long modified, 
		Long validTill, Boolean secuity, Boolean sentiment) throws Exception {

		DocMeta meta = new DocMeta();
		meta.docType = type;
		meta.state = state;
		meta.tenant = orgunit;
		meta.geoHouse= geohouse;
		meta.createdOn = new Date(created);
		meta.modifiedOn = new Date(modified);
		meta.validTill = new Date(validTill);
		meta.securityHigh = secuity;
		meta.sentimentPositive = sentiment;
		
		byte[] bytes = meta.toBytes();
		DocMeta deserialized = new DocMeta(bytes);
		
		assertEquals(type, deserialized.docType);
		assertEquals(state, deserialized.state);
		assertEquals(orgunit, deserialized.tenant);
		assertEquals(geohouse, deserialized.geoHouse);

		assertEquals(created.longValue(), deserialized.createdOn.getTime());
		assertEquals(modified.longValue(), deserialized.modifiedOn.getTime());
		assertEquals(validTill.longValue(), deserialized.validTill.getTime());
		
		assertEquals(secuity.booleanValue(), deserialized.securityHigh);
		assertEquals(sentiment.booleanValue(), deserialized.sentimentPositive);
		
	}
	
}
