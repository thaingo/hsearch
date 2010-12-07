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

import java.io.IOException;
import java.io.Writer;
import java.util.List;

import com.bizosys.hsearch.common.HDocument;
import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.hbase.HReader;
import com.bizosys.hsearch.hbase.NVBytes;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;

/**
 * Documents are the unit of indexing and search. 
 * A Document consists of: 
 * <lu>
 * 	<li>Set of fields</li>
 * 	<li>Access information</li>
 * 	<li>Meta information</li>
 * 	<li>Result Display Section</li>
 * </lu>
 * <br/> * A document is uniquely identified by the doc merging Id (Bucket) 
 * and the document serial number inside the bucket.  
 * @author karan
 *
 */
public class Doc {
	
	/**
	 * Term vectors created after parsing the document
	 */
	public DocTerms terms = null;
	
	/**
	 * The document meta section
	 */
	public DocMeta meta = null;
	
	/**
	 * Document view and edit access control settings
	 */
	public DocAcl acl = null;
	
	/**
	 * The result display formats
	 */
	public DocTeaser teaser = null;
	
	/**
	 * The content section which consists of fields
	 */
	public DocContent content = null;
	
	/**
	 * From which machine the document is submitted
	 */
	public String ipAddress = null;
	
	/**
	 * The 
	 */
	public Long bucketId = null;
	public Short docSerialId = null;

	public Doc() {
	}
	
	public Doc(HDocument hDoc) throws SystemFault, ApplicationFault{
		this.bucketId = hDoc.bucketId;
		this.docSerialId = hDoc.docSerialId;
		this.ipAddress = hDoc.ipAddress;
		
		this.meta = new DocMeta(hDoc);
		this.teaser = new DocTeaser(hDoc);
		this.content = new DocContent(hDoc);
		this.acl = new DocAcl(hDoc);
		this.terms = new DocTerms();
	}
	
	public Doc(String origId) throws SystemFault, ApplicationFault {
		
		/**
		 * Get the mapped Id
		 */
		List<NVBytes> mappingB = IdMapping.getKey(origId.getBytes());
		if ( null == mappingB) throw new ApplicationFault("Id not found :" + origId);
		if ( 1 != mappingB.size()) throw new ApplicationFault(mappingB.size() + " Ids found :" + origId);
		String mappedKey = new String(mappingB.get(0).data);
		this.bucketId = IdMapping.getBucket(mappedKey);
		this.docSerialId = IdMapping.getDocSerial(mappedKey);
		mappingB.clear();

		/**
		 * Get the Content
		 */
		List<NVBytes> contentB = HReader.getCompleteRow(IOConstants.TABLE_CONTENT, mappedKey.getBytes());
		if ( null != contentB) {
			this.content = new DocContent(contentB );
			contentB.clear();
		}
		
		/**
		 * Get the Meta
		 */
		List<NVBytes> previewB = HReader.getCompleteRow(
			IOConstants.TABLE_PREVIEW, mappedKey.getBytes());
		if ( null != previewB) {
			this.teaser = new DocTeaser(origId.getBytes(), previewB);
			this.teaser.id = new Storable(origId);
			for (NVBytes nv : previewB) {
				if ( Storable.compareBytes(nv.name, IOConstants.META_BYTES)) 
					this.meta = new DocMeta(nv.data);
				else if ( Storable.compareBytes(nv.name, IOConstants.ACL_BYTES))
					this.acl = new DocAcl(nv.data);
			}
			previewB.clear();
		}
	}
	
	
	/**
	 * Recycles this document.
	 * Helps GC to garbase collect better.
	 *
	 */
	public void recycle() {
		this.terms.cleanup();
		this.meta.cleanup();
		this.acl.cleanup();
		this.teaser.cleanup();
		this.content.cleanup();
		bucketId = null;
		docSerialId = 0;
	}
	

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(">>>> Document Starts <<<<");
		if ( null != bucketId ) sb.append("\n Bucket :").append(bucketId.toString());
		if ( null != docSerialId ) sb.append("\n Doc Serial :").append(docSerialId);
		if ( null != terms ) sb.append("\n Term :").append(terms.toString());
		if ( null != acl ) sb.append("\n Acl : ").append(acl.toString());
		if ( null != meta ) sb.append("\n Meta :").append(meta.toString());
		if ( null != teaser ) sb.append("\n Teaser:").append(teaser.toString());
		if ( null != content ) sb.append("\n Content").append(content.toString());
		sb.append("\n>>>> Document Ends <<<<\n");
		return sb.toString();
	}
	
	public void toXml(Writer writer) throws IOException {
		if ( null != bucketId ) writer.append("<b>").append(bucketId.toString()).append("</b>");
		if ( null != docSerialId ) writer.append("<n>").append(docSerialId.toString()).append("</n>");
		//if ( null != acl ) writer.append("<a>").append(acl.toString()).append("</a>");
		if ( null != meta ) meta.toXml(writer);
		if ( null != teaser ) teaser.toXml(writer);
		if ( null != content ) content.toXml(writer);
	}
}
