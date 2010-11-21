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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.StringTokenizer;

import com.bizosys.hsearch.common.HDocument;
import com.bizosys.hsearch.common.IStorable;
import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.util.StringUtils;

/**
 * An empty meta is currently only 6 byte length.
 * @author karan
 *
 */
public class DocMeta implements IStorable, IDimension {
	
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
	 * The Geo House.
	 */
	public String geoHouse = null;

	/**
	 * Document weight : Integer which biases the ranking algorithm.
	 * Document weight is lifted based on it's depth, source 
	 * A home page will have more weight than the deeper location.
	 * documents from Intel page will have more weight 
	 * This could be manullay increased to influence the ranking mechanism 
	 */
	public int weight = 0;

	/**
	 * Document Type 
	 * Table Name / File Extension / Dna Name
	 */
	public String docType = null;

	/**
	 * These are author keywords or meta section of the page
	 */
	public String tags = null;

	/**
	 * These are user keywords formed from the search terms
	 */
	public String socialText = null;

	
	/**
	 * Which date the document is created. 
	 */
	public Date createdOn = null;

	/**
	 * Which date the document is last updated. 
	 */
	public Date modifiedOn = null;
	
	/**
	 * Till what date this document is valid
	 */
	public Date validTill = null;
	
	/**
	 * From which IP address is this document created. 
	 * This is specially for machine proximity ranking. 
	 */
	public int ipHouse = 0;

	/**
	 * High Security setting. During high security, 
	 * the information kept encrypted. 
	 */
	public boolean securityHigh = false;

	
	/**
	 * By default the sentiment is positive. 
	 */
	public boolean sentimentPositive = true;
	
	public Locale locale = Locale.ENGLISH;
	
	/**
	 * Default Constructor
	 *
	 */
	public DocMeta() {
	}
	
	public DocMeta(HDocument meta) {
		if ( null != meta.tags) 
			this.tags  = StringUtils.listToString(meta.tags, '\t') ;
		this.createdOn = meta.bornOn;
		this.validTill = meta.deathOn;
		this.docType = meta.docType;
		this.eastering = meta.eastering;
		this.modifiedOn = meta.modifiedOn;
		this.northing = meta.northing;
		this.tenant = meta.tenant;
		if ( null != meta.socialText) 
			this.socialText = StringUtils.listToString(meta.socialText, '\t') ;
		this.securityHigh = meta.securityHigh;
		this.sentimentPositive = meta.sentimentPositive;
		this.state = meta.state;
		this.weight = meta.weight;
		
		this.locale = meta.locale;
	}	
	
	/**
	 * Read the meta information from the byte array.
	 * Deserialize and initiate
	 * @param bytes : Serialized bytes
	 */
	public DocMeta(byte[] bytes) {
		int pos = 0;
		
		byte docTypeLen = bytes[pos];
		pos++;
		byte[] docTypeB = new byte[docTypeLen];
		System.arraycopy(bytes, pos, docTypeB, 0, docTypeLen);
		this.docType = Storable.getString(docTypeB);
		pos = pos + docTypeLen;
		
		byte stateLen = bytes[pos];
		pos++;
		byte[] stateB = new byte[stateLen];
		System.arraycopy(bytes, pos, stateB, 0, stateLen);
		this.state = Storable.getString(stateB);
		pos = pos + stateLen;
		
		byte orgUnitLen = bytes[pos];
		pos++;
		byte[] orgUnitB = new byte[orgUnitLen];
		System.arraycopy(bytes, pos, orgUnitB, 0, orgUnitLen);
		this.tenant = Storable.getString(orgUnitB);
		pos = pos + orgUnitLen;
		
		byte geoHouseLen = bytes[pos];
		pos++;
		byte[] geoHouseB = new byte[geoHouseLen];
		System.arraycopy(bytes, pos, geoHouseB, 0, geoHouseLen);
		this.geoHouse = Storable.getString(geoHouseB);
		pos = pos + geoHouseLen;
		
		byte flag_1B = bytes[pos++];
		boolean[] flag_1 = Storable.byteToBits(flag_1B);
		
		byte flag_2B = bytes[pos++];
		boolean[] flag_2 = Storable.byteToBits(flag_2B);
		
		int bitPos = 0;
		if ( flag_1[bitPos++]) {
			this.eastering = Float.intBitsToFloat(Storable.getInt(pos, bytes));
			pos = pos+ 4;
		}
		
		if ( flag_1[bitPos++]) {
			this.northing = Float.intBitsToFloat(Storable.getInt(pos, bytes));
			pos = pos+ 4;
		}
		
		if ( flag_1[bitPos++]) {
			this.weight = Storable.getInt(pos, bytes);
			pos = pos+ 4;
		}
		
		if ( flag_1[bitPos++]) {
			this.ipHouse = Storable.getInt(pos, bytes);
			pos = pos+ 4;
		}
		
		this.securityHigh = flag_1[bitPos++];
		this.sentimentPositive = flag_1[bitPos++];
		
		if (flag_1[bitPos++]) {
			short len = Storable.getShort(pos, bytes);
			pos = pos + 2;
			byte[] tagsB = new byte[len];
			System.arraycopy(bytes, pos, tagsB, 0, len);
		    this.tags = Storable.getString(tagsB);
			pos = pos + tagsB.length;
		}
		
		if (flag_1[bitPos++]) {
			short len = Storable.getShort(pos, bytes);
			pos = pos + 2;
			byte[] socialTextB = new byte[len];
			System.arraycopy(bytes, pos, socialTextB, 0, len);
		    this.socialText = Storable.getString(socialTextB);
			pos = pos + socialTextB.length;
		}
		
		bitPos = 0;
		if (flag_2[bitPos++]) {
			this.createdOn = new Date(Storable.getLong(pos, bytes));
			pos = pos+ 8;
		}
		
		if (flag_2[bitPos++]) {
			this.modifiedOn = new Date(Storable.getLong(pos, bytes));
			pos = pos+ 8;
		}
		
		if (flag_2[bitPos++]) {
			this.validTill = new Date(Storable.getLong(pos, bytes));
			pos = pos+ 8;
		}
	}
	
	/**
	 * Filteration criteria
	 */
	public boolean checkActive(Date fromDate, Date toDate) {
		return ( (this.modifiedOn.after(fromDate)) && 
			this.modifiedOn.before(toDate)) ;
	}
	
	/**
	 * Returns all the necessary fields for processing.
	 * orgUnit is treated specially. It goes in a column
	 * This helps to search just on orgUnit fields and then
	 * retrieve documents.
	 * 
	 *  It stores type.. If the type is * means matches all
	 *  
	 */
	public byte[] toBytes() {
		byte docTypeLen = (byte) 0;
		byte[] docTypeB = null;
		if ( null != this.docType) {
			docTypeB = Storable.putString(this.docType);
			docTypeLen = (byte) docTypeB.length;
		}
		
		byte stateLen = (byte) 0;
		byte[] stateB = null;
		if ( null != this.state) {
			stateB = Storable.putString(this.state);
			stateLen = (byte) stateB.length;
		}
		
		byte orgUnitLen = (byte) 0;
		byte[] orgUnitB = null;
		if ( null != this.tenant) {
			orgUnitB = Storable.putString(this.tenant);
			orgUnitLen = (byte) orgUnitB.length;
		}
		
		byte geoHouseLen = (byte) 0;
		byte[] geoHouseB = null;
		if ( null != this.geoHouse) {
			geoHouseB = Storable.putString(this.geoHouse);
			geoHouseLen = (byte) geoHouseB.length;
		}
		
		boolean isNorthing = false;
		byte[] northingB = null;
		if ( this.northing != 0.0f) {
			isNorthing = true;
			northingB = Storable.putInt(Float.floatToIntBits(this.northing));
		}

		boolean isEastering = false;
		byte[] easteringB = null;
		if ( this.eastering != 0.0f) {
			isEastering = true;
			easteringB = Storable.putInt(Float.floatToIntBits(this.eastering));
		}
		
		boolean isWeight = false;
		byte[] weightB = null;
		if ( this.weight != 0) {
			isWeight = true;
			weightB = Storable.putInt(this.weight);
		}
		
		boolean isIpHouse = false;
		byte[] iphouseB = null;
		if ( this.ipHouse != 0) {
			isIpHouse = true;
			iphouseB = Storable.putInt(this.ipHouse);
		}
		
		boolean isTags = false;
		byte[] tagsB = null;
		if ( null != this.tags ) {
			isTags = true;
			tagsB = Storable.putString(this.tags);
		}
		
		boolean isSocialText = false;
		byte[] socialTextB = null;
		if ( null != this.socialText ) {
			isSocialText = true;
			socialTextB  = Storable.putString(this.socialText);
		}
		
		boolean isBornOn = false;
		byte[] bornOnB = null;
		if ( null != this.createdOn) {
			isBornOn = true;
			bornOnB = Storable.putLong(this.createdOn.getTime());
		}
		
		boolean isModifiedOn = false;
		byte[] modifiedOnB = null;
		if ( null != this.modifiedOn) {
			isModifiedOn = true;
			modifiedOnB = Storable.putLong(this.modifiedOn.getTime());
		}
		
		boolean isDeathOn = false;
		byte[] deathOnB = null;
		if ( null != this.validTill) {
			isDeathOn = true;
			deathOnB = Storable.putLong(this.validTill.getTime());
		}
		
		byte flag_1 = Storable.bitsToByte(new boolean[] {
			isEastering, isNorthing, isWeight, isIpHouse, securityHigh, sentimentPositive, isTags, isSocialText});
		
		byte flag_2 = Storable.bitsToByte(new boolean[] {
			isBornOn, isModifiedOn, isDeathOn, false, false, false, false, false});
		
		int totalBytes = 1 /** docTypeLen */ + 
			1 /** stateLen */ + 1 /** orgUnitLen */ + 1 /** geoHouseLen */ +
			1 /** dataPresence */ + 1  /** timePresence */ + 
			docTypeLen + stateLen + orgUnitLen + geoHouseLen;
		if ( isEastering) totalBytes = totalBytes + 4;
		if ( isNorthing ) totalBytes = totalBytes + 4;
		if ( isWeight  ) totalBytes = totalBytes + 4;
		if ( isIpHouse  ) totalBytes = totalBytes + 4;
		if ( isTags  ) totalBytes = totalBytes + tagsB.length + 2;
		if ( isSocialText ) totalBytes = 
			totalBytes + socialTextB.length + 2;
			
		if ( isBornOn ) totalBytes = totalBytes + 8;
		if ( isModifiedOn ) totalBytes = totalBytes + 8;
		if ( isDeathOn ) totalBytes = totalBytes + 8;
		
		/**
		 * Writing Start
		 */
		byte[] bytes = new byte[totalBytes];
		int pos = 0;
		
		bytes[pos++] = docTypeLen;
		if ( 0 != docTypeLen)
			System.arraycopy(docTypeB, 0, bytes, pos, docTypeLen);
		pos = pos + docTypeLen;
		
		bytes[pos++] = stateLen;
		if ( 0 != stateLen)
			System.arraycopy(stateB, 0, bytes, pos, stateLen);
		pos = pos + stateLen;
		
		bytes[pos++] = orgUnitLen;
		if ( 0 != orgUnitLen)
			System.arraycopy(orgUnitB, 0, bytes, pos, orgUnitLen);
		pos = pos + orgUnitLen;
		
		bytes[pos++] = geoHouseLen;
		if ( 0 != geoHouseLen)
			System.arraycopy(geoHouseB, 0, bytes, pos, geoHouseLen);
		pos = pos + geoHouseLen;
		
		bytes[pos] = flag_1;
		pos++;
		
		bytes[pos] = flag_2;
		pos++;
		
		if ( isEastering) {
			System.arraycopy(easteringB, 0, bytes, pos, 4);
			pos = pos+ 4;
		}
		
		if ( isNorthing ) {
			System.arraycopy(northingB, 0, bytes, pos, 4);
			pos = pos+ 4;
		}
		
		if (isWeight) {
			System.arraycopy(weightB, 0, bytes, pos, 4);
			pos = pos+ 4;
		}
		
		if ( isIpHouse) {
			System.arraycopy(iphouseB, 0, bytes, pos, 4);
			pos = pos+ 4;
		}
		
		if (isTags) {
			System.arraycopy(Storable.putShort((short)tagsB.length), 0, bytes, pos, 2);
			pos = pos + 2;
			System.arraycopy(tagsB, 0, bytes, pos, tagsB.length);
			pos = pos+ tagsB.length;
		}
		
		if (isSocialText) {
			System.arraycopy(Storable.putShort((short)socialTextB.length), 0, bytes, pos, 2);
			pos = pos + 2;
			System.arraycopy(socialTextB, 0, bytes, pos, socialTextB.length);
			pos = pos+ socialTextB.length;
		}
		
		if (isBornOn) {
			System.arraycopy(bornOnB, 0, bytes, pos, 8);
			pos = pos+ 8;
		}
		
		if (isModifiedOn) {
			System.arraycopy(modifiedOnB, 0, bytes, pos, 8);
			pos = pos+ 8;
		}
		
		if(isDeathOn) {
			System.arraycopy(deathOnB, 0, bytes, pos, 8);
			pos = pos+ 8;
		}
		
		return bytes;
	}

	/**
	 * Cleans up the entire set and make it available for reuse.
	 */
	public void cleanup() {
		this.state = null;
		this.tenant = null;
		this.northing = 0.0f;
		this.eastering = 0.0f;
		this.weight = 0;
		this.docType = null;
		this.securityHigh = false;
		this.tags = null;
		this.socialText = null;
		this.createdOn = null;
		this.modifiedOn = null;
		this.validTill = null;
		this.ipHouse = 0;
		this.geoHouse = null;
	}
	
	@Override
	public String toString() {
		StringBuilder writer = new StringBuilder();
		writer.append("<m>");
		if ( null != this.tags) {
			writer.append("<a>").append(this.tags).append("</a>");
		}
		if ( null != this.createdOn ) writer.append("<b>").append(this.createdOn.toString()).append("</b>");
		if ( null != this.validTill) writer.append("<d>").append(this.validTill.toString()).append("</d>");
		if ( StringUtils.isNonEmpty(this.geoHouse) ) writer.append("<g>").append(this.geoHouse).append("</g>");
		if ( null != this.modifiedOn) writer.append("<m>").append(this.modifiedOn.toString()).append("</m>");
		if ( StringUtils.isNonEmpty(this.tenant) ) writer.append("<o>").append(this.tenant).append("</o>");
		if ( null != this.socialText) {
			writer.append("<r>").append(this.socialText).append("</r>");
		}
		if ( StringUtils.isNonEmpty(this.state) ) writer.append("<s>").append(this.state).append("</s>");
		if ( StringUtils.isNonEmpty(this.docType) ) writer.append("<t>").append(this.docType).append("</t>");
		writer.append("<x>").append(securityHigh).append("</x>");
		writer.append("<z>").append(sentimentPositive).append("</z>");
		writer.append("</m>");
		return writer.toString();
	}

	public void toNVs(List<NV> nvs) throws ApplicationFault {
		nvs.add(new NV(IOConstants.SEARCH_BYTES,IOConstants.META_BYTES, this));
	}
	
	public void addTags(List<String> tagL) {
		if (this.tags == null) {
			this.tags = StringUtils.listToString(tagL, '\t') ; 
		} else {
			this.tags = this.tags + '\t' +  
				StringUtils.listToString(tagL, '\t') ;
		}
	}

	public List<String> getTags() {
		if ( null == tags) return null;
		List<String> tagL = new ArrayList<String>();
		StringTokenizer tokenizer = new StringTokenizer(tags,"\t");
	    while (tokenizer.hasMoreTokens()) {
	    	tagL.add(tokenizer.nextToken());
	    }
	    return tagL;
	}
	
	
	public void addSocialText(List<String> socialText) {
		if (this.socialText == null) {
			this.socialText = StringUtils.listToString(socialText, '\t') ; 
		} else {
			this.socialText = this.socialText + '\t' +  
				StringUtils.listToString(socialText, '\t') ;
		}
	}

	public List<String> getSocialText() {
		if ( null == socialText) return null;
		List<String> socialTextL = new ArrayList<String>();
		StringTokenizer tokenizer = new StringTokenizer(socialText,"\t");
	    while (tokenizer.hasMoreTokens()) {
	    	socialTextL.add(tokenizer.nextToken());
	    }
	    return socialTextL;
	}
}
