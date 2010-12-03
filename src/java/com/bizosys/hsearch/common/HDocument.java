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

import com.bizosys.hsearch.filter.AccessDefn;


public class HDocument {

	/**
	 * Mapped Bucket and Document Serial Numbers
	 */
	public Long bucketId = null;
	public Short docSerialId = null;
	

	/**
	 * The Document ID, This is s a unique ID.
	 * This ID could be later used 
	 * 
	 * get/update/delete the document
	 * Mapping to original document
	 */
	public String originalId =  null;
	
	/**
	 * Where is this document located
	 */
	public String url =  null;

	/**
	 * The title of the document
	 */
	public String title =  null; 
	
	/**
	 * The Preview text on the document
	 */
	public String preview =  null;
	
	/**
	 * The Preview text on the document
	 */
	public String cacheText =  null;

	/**
	 * List all fields of this document
	 */
	public List<Field> fields = null;
	
	/**
	 * To which the document has cited
	 */
	public List<String> citationTo =  null;

	/**
	 * From which the document has cited
	 */
	public List<String> citationFrom =  null;
	
	
	/**
	 * Who has view access to the document
	 */
	public AccessDefn viewPermission = null;
	
	/**
	 * Who has edit access to the document
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
	 * Northing of a place
	 */
	public Float northing = 0.0f;

	/**
	 * Eastering of a place
	 */
	public Float eastering = 0.0f;

	/**
	 * This weight is editor assigned weight
	 */
	public int weight = 0;

	/**
	 * Document Type 
	 * Table Name / XML record type
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
	 * Which date the document is created. 
	 */
	public Date createdOn = null;

	/**
	 * Which date the document is last updated. 
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
	 * Which language are these documents. Let this comes before hand
	 */
	public Locale locale = Locale.ENGLISH;
	
	public StorableList getCitationTo() {
		if ( null == this.citationTo) return null;
		StorableList storable = new StorableList();
		for (String strCitation : this.citationTo) {
			storable.add(strCitation);
		}
		return storable;
	}
	
	public StorableList getCitationFrom() {
		if ( null == this.citationFrom) return null;
		StorableList storable = new StorableList();
		for (String strCitation : this.citationFrom) {
			storable.add(strCitation);
		}
		return storable;
	}
}
