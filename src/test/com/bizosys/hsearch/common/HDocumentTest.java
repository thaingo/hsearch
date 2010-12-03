package com.bizosys.hsearch.common;

import java.util.ArrayList;
import java.util.Date;
import java.util.Locale;

import com.bizosys.hsearch.filter.AccessDefn;
import com.bizosys.oneline.util.XmlUtils;

public class HDocumentTest {
	public static void main(String[] args) {
		HDocument doc = new HDocument();
		doc.bucketId = 99999L;
		doc.docSerialId = (short) 12;
		doc.cacheText = "This is cache text";
		doc.citationFrom = new ArrayList<String>();
		doc.citationFrom.add("Cited From paper1");
		doc.citationFrom.add("Cited From paper2");
		
		doc.citationTo = new ArrayList<String>();
		doc.citationTo.add("Cited To Paper1");
		doc.citationTo.add("Cited To Paper2");
		
		doc.createdOn = new Date();
		doc.docType = "doctype1";
		doc.weight = 11;
		doc.eastering = 100012.23F;
		doc.northing = 200012.23F;
		doc.editPermission = new AccessDefn();
		doc.editPermission.uids = new String[] {"n4501"};
		doc.editPermission.teams = new String[] {"teamA"};
		
		doc.fields = new ArrayList<Field>();
		doc.fields.add(new SField(true,true,true,Storable.BYTE_STRING,"fld1","value1"));
		doc.fields.add(new SField(true,true,true,Storable.BYTE_INT,"fld2","199"));
		
		doc.ipAddress = "192.168.2.3";
		doc.locale = Locale.ENGLISH;
		
		doc.modifiedOn = new Date();
		doc.northing = 23.44F;
		doc.originalId = "ORIG123";
		doc.preview = "<b>I am cool</b>";
		doc.securityHigh = false;
		doc.sentimentPositive = false;
		doc.socialText = new ArrayList<String>();
		doc.socialText.add("universe");
		doc.socialText.add("bob kamath");
		
		doc.state = "active";
		doc.tags = new ArrayList<String>();
		doc.tags.add("sociology");
		doc.tags.add("biology");
		
		doc.tenant = "bizosys";
		doc.title = "Title Text";
		doc.url = "http://wwww.google.com";
		
		doc.validTill = new Date();
		doc.viewPermission = new AccessDefn();
		doc.viewPermission.roles = new String[] {"Role1"};
		doc.viewPermission.ouAndRoles = new String[][] { new String[]{"unit1", "role1"} };
		
		doc.weight = 99;
    	String xmlDoc = XmlUtils.xstream.toXML(doc);
		HDocument docDeserialized = (HDocument) 
			XmlUtils.xstream.fromXML(xmlDoc);
		
    	String xmlDocSerialized = XmlUtils.xstream.toXML(docDeserialized);
		System.out.println(xmlDoc.equals(xmlDocSerialized));
	}
}
