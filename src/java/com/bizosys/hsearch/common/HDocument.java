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
package com.bizosys.hsearch.common;

import java.util.Date;
import java.util.List;
import java.util.Locale;

import com.bizosys.hsearch.util.GeoId;


/**
 * This object carries all information necessary for indexing a document.
 * This object is also serializable and client can provide it as a 
 * XML document (REST API).
 * @see GeoId
 */
public class HDocument {

	/**
	 * Document Merged Storage(Bucket) Number  
	 */
	public Long bucketId = null;
	
	/**
	 * Document Serial number inside the merged storage (Bucket)
	 */
	public Short docSerialId = null;
	

	/**
	 * This is the original Id of the document.
	 * This id usually flows from the original document source
	 * e.g. Primary Key of a database table. The mapped bucket Id and
	 * document serial number inside bucket represents uniqueness inside
	 * the index. 
	 */
	public String originalId =  null;
	
	/**
	 * URL for accessing the document directly
	 */
	public String url =  null;

	/**
	 * Document title. This also shows in the search result record title
	 */
	public String title =  null; 
	
	/**
	 * The Preview text on the document. It can be URL to an image or inline
	 * XML information.
	 */
	public String preview =  null;
	
	/**
	 * The matching section of the search word occurance is picked from
	 * the cached text sections
	 */
	public String cacheText =  null;

	/**
	 * Document content Fields
	 */ 
	public List<Field> fields = null;
	
	/**
	 * Manually supplied list of citation mentioned in the document
	 */
	public List<String> citationTo =  null;

	/**
	 * Manually supplied list of citations from other documents
	 */
	public List<String> citationFrom =  null;
	
	/**
	 * Who has view access to this document
	 */
	public AccessDefn viewPermission = null;
	
	/**
	 * Who has edit access of this document
	 */
	public AccessDefn editPermission = null;

	/**
	 * The state of the docucment (Applied, Processed, Active, Inactive)
	 */
	public String state = null;
	
	/**
	 * Just the Organization Unit (HR, PRODUCTION, SI)
	 * If there are multi level separate it with \ or .
	 */
	public String tenant = null;

	/**
	 * Easting refers to the eastward-measured distance (or the x-coordinate)
	 * Use <code>GeoId.convertLatLng</code> method for getting nothing eastering
	 * from a given latitude and longitude.
	 */
	public Float eastering = 0.0f;

	/**
	 * northing refers to the northward-measured distance (or the y-coordinate). 
	 * Use <code>GeoId.convertLatLng</code> method for getting nothing eastering
	 * from a given latitude and longitude.
	 */
	public Float northing = 0.0f;

	/**
	 * This Default weight of the document. Few examples for computing the weight are
	 * <lu>
	 * 	<li>Editor assigned</li>
	 * 	<li>Default weight assigned to the document source e.g. pages from wikipedia.org</li>
	 * 	<li>Default weight assigned to the document editor e.g. blogs from CEO</li>
	 * </lu> 
	 * 
	 */
	public int weight = 0;

	/**
	 * Document Type. It's the record type.
	 * Use  <code>DocumentType</code> class to define default document types.
	 */
	public String docType = null;

	/**
	 * These are author keywords or meta section of the page
	 */
	public List<String> tags = null;

	/**
	 * These are user keywords formed from the search terms
	 */
	public List<String> socialText = null;

	/**
	 * Document creation date 
	 */
	public Date createdOn = null;

	/**
	 * Document updation date 
	 */
	public Date modifiedOn = null;
	
	/**
	 * When the document is scheduled to die or died
	 */
	public Date validTill = null;
	
	/**
	 * From which IP address is this document created. 
	 * This is specially for machine proximity ranking. 
	 */
	public String ipAddress = null;

	/**
	 * High Security setting. During high security, 
	 * the information kept encrypted. 
	 */
	public boolean securityHigh = false;

	
	/**
	 * By default the sentiment is positive. 
	 */
	public boolean sentimentPositive = true;
	
	/**
	 * Document Language. Default is English 
	 */
	public Locale locale = Locale.ENGLISH;

	public HDocument() {
		
	}
	
	public HDocument(String id, String title) {
		this.originalId = id;
		this.title = title;
	}
}
