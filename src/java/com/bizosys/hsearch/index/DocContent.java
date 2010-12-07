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
import java.util.ArrayList;
import java.util.List;

import com.bizosys.hsearch.common.ByteField;
import com.bizosys.hsearch.common.Field;
import com.bizosys.hsearch.common.HDocument;
import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.common.StorableList;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.hbase.NVBytes;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;

/**
 * Stores the fields of the document. This also stores additional
 * citation information about the document.
 * @author bizosys
 *
 */
public class DocContent implements IDimension {

	/**
	 * The content, It could be inline or a URL
	 */
	public List<ByteField> stored =  null;
	public List<ByteField> analyzedIndexed =  null;
	public List<ByteField> nonAnalyzedIndexed =  null;
	
	/**
	 * To which the document has cited
	 */
	public StorableList citationTo =  null;

	/**
	 * From which the document has cited
	 */
	public StorableList citationFrom =  null;
	
	
	public DocContent(HDocument aDoc) throws SystemFault{
		if(null == aDoc) return;
		
		this.citationTo = getCitationTo(aDoc);
		this.citationFrom = getCitationFrom(aDoc);

		if(null == aDoc.fields) return;
		for (Field fld: aDoc.fields) {
			if ( fld.isStore()) {
				if ( null == this.stored) this.stored = new ArrayList<ByteField>(); 
				this.stored.add(fld.getByteField());
			} 
			
			if ( fld.isIndexable()) {
				if (fld.isAnalyze()) {
					if ( null == this.analyzedIndexed) this.analyzedIndexed = new ArrayList<ByteField>();
					this.analyzedIndexed.add(fld.getByteField());
				} else {
					if ( null == this.nonAnalyzedIndexed) this.nonAnalyzedIndexed = new ArrayList<ByteField>(); 
					this.nonAnalyzedIndexed.add(fld.getByteField());
				}
			}
		}
	}
	
	public DocContent (List<NVBytes> nvs) throws ApplicationFault {
		if ( null == nvs) return;
		
		for (NVBytes nv : nvs) {
			switch(nv.family[0]) {
				case IOConstants.CONTENT_CITATION:
					switch(nv.name[0]) {
						case IOConstants.CONTENT_CITATION_TO:
							this.citationTo = new StorableList(nv.data);
							break;
						case IOConstants.CONTENT_CITATION_FROM:
							this.citationFrom = new StorableList(nv.data);
							break;
						default:
							throw new ApplicationFault("DocContent: Unknown Citation Field");
					}
					break;
					
				case IOConstants.CONTENT_FIELDS:
					if ( null == this.stored) this.stored = new ArrayList<ByteField>();
					this.stored.add(ByteField.wrap(nv.name, nv.data));
					break;
					
				default:
					throw new ApplicationFault("DocContent : Unklnow Field " + nv.toString());
			}
		}
	}	

	public void toNVs(List<NV> nvs) {
		if ( null != this.stored ) {
			for (ByteField bf : this.stored) {
				bf.enableTypeOnToBytes(true);
				nvs.add(new NV(
					IOConstants.CONTENT_FIELDS_BYTES, bf.getName(), new Storable(bf.toBytes())
					)
				);
			}
		}
		
		if ( null != this.citationTo ) nvs.add( new NV(
			IOConstants.CONTENT_CITATION_BYTES,
			IOConstants.CONTENT_CITATION_TO_BYTES, this.citationTo ) );

		if ( null != this.citationFrom ) nvs.add( new NV(
			IOConstants.CONTENT_CITATION_BYTES,
			IOConstants.CONTENT_CITATION_TO_BYTES, this.citationFrom ) );		
	}
	
	private StorableList getCitationTo(HDocument aDoc) {
		if ( null == this.citationTo) return null;
		StorableList storable = new StorableList();
		for (String strCitation : aDoc.citationTo) {
			storable.add(strCitation);
		}
		return storable;
	}
	
	private StorableList getCitationFrom(HDocument aDoc) {
		if ( null == this.citationFrom) return null;
		StorableList storable = new StorableList();
		for (String strCitation : aDoc.citationFrom) {
			storable.add(strCitation);
		}
		return storable;
	}	
	
	/**
	 * Clean up the entire set.
	 */
	public void cleanup() {
		if ( null != this.stored ) this.stored.clear();
		this.citationTo =  null; 
		this.citationFrom =  null;
	}
	
	public void toXml(Writer writer) throws IOException {
		writer.append("<c>");
		if ( null != this.stored ) {
			for (ByteField bf: this.stored) {
				writer.append('<').append(bf.name).append('>');
				writer.append(bf.getValue().toString());
				writer.append("</").append(bf.name).append('>');
			}
		}
		
		writer.append("<t>");
		for ( Object obj: this.citationTo) writer.append(obj.toString());
		writer.append("</t>");

		writer.append("<f>");
		for ( Object obj: this.citationFrom) writer.append(obj.toString());
		writer.append("</f>");
		
		writer.append("</c>");
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder('\n');
		if ( null != this.stored ) {
			for (ByteField bf: this.stored) {
				sb.append("Fields:").append(bf);
			}
		}
		if ( null != this.citationTo ) sb.append("Citation To :").append(new String(this.citationTo.toString()));
		if ( null != this.citationFrom ) sb.append("Citation From :").append(new String(this.citationFrom.toString()));
		return sb.toString();
	}


}
