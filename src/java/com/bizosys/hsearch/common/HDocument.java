package com.bizosys.hsearch.common;

import java.util.Date;
import java.util.List;
import java.util.Locale;

import com.bizosys.hsearch.filter.Access;


public class HDocument {

	/**
	 * The Document ID, This is s a unique ID.
	 * This ID could be later used 
	 * 
	 * get/update/delete the document
	 * Mapping to original document
	 */
	public Storable originalId =  null;
	
	/**
	 * Mapped Bucket and Document Serial Numbers
	 */
	public Long bucketId = null;
	public Short docSerialId = null;
	
	/**
	 * Where is this document located
	 */
	public Storable url =  null;

	/**
	 * The title of the document
	 */
	public Storable title =  null; 
	
	/**
	 * The Preview text on the document
	 */
	public Storable preview =  null;
	
	/**
	 * List all fields of this document
	 */
	public List<HField> fields = null;
	
	/**
	 * To which the document has cited
	 */
	public StorableList citationTo =  null;

	/**
	 * From which the document has cited
	 */
	public StorableList citationFrom =  null;
	
	
	/**
	 * Who has edit access to the document
	 */
	public Access editAcl = null;
	
	/**
	 * Who has view access to the document
	 */
	public Access viewAcl = null;

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
	public Date bornOn = null;

	/**
	 * Which date the document is last updated. 
	 */
	public Date modifiedOn = null;
	
	/**
	 * When the document is scheduled to die or died
	 */
	public Date deathOn = null;
	
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
}
