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
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.hbase.NVBytes;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.hsearch.util.UrlMapper;
import com.bizosys.oneline.ApplicationFault;

/**
 * Documents could be coming as structured or unstructured.
 * @author bizosys
 *
 */
public class DocTeaser {

	/**
	 * The Document ID
	 */
	public String mappedId =  null;
	
	/**
	 * The Document ID
	 */
	public Storable id =  null;
	
	/**
	 * Where is this document located
	 */
	public Storable url =  null;

	/**
	 * The title of the document
	 */
	public Storable title =  null; 
	
	/**
	 * Cached Text
	 */
	public Storable cacheText =  null;
	
	/**
	 * The body text of the document
	 */
	public Storable preview =  null;

	public DocTeaser() {
	}
	
	/**
	 * Formulates the document teaser from the supplied Fields of a document. 
	 * @param aDoc
	 */
	public DocTeaser(HDocument aDoc) throws ApplicationFault {
		if ( null == aDoc.originalId) throw new ApplicationFault("Document Id is not present."); 
			this.id = new Storable(aDoc.originalId);
		if ( null != aDoc.url) this.url = new Storable(aDoc.url);
		if ( null != aDoc.title) this.title =   new Storable(aDoc.title);
		if ( null != aDoc.cacheText ) this.preview =  new Storable(aDoc.preview);
		if ( null != aDoc.cacheText ) this.cacheText = new Storable(aDoc.cacheText);
	}
	
	public DocTeaser (byte[] id, List<NVBytes> inputBytes) throws ApplicationFault {
		
		this.mappedId = Storable.getString(id);
		if ( null == inputBytes) return;
		
		for (NVBytes fld : inputBytes) {
			switch(fld.name[0]) {
				case IOConstants.TEASER_ID:
					this.id = new Storable(fld.data);
					break;
				case IOConstants.TEASER_URL:
					String codedUrl = Storable.getString(fld.data);
					setUrl(UrlMapper.getInstance().decoding(codedUrl));
					break;
				case IOConstants.TEASER_TITLE:
					setTitle(Storable.getString(fld.data));
					break;
				case IOConstants.TEASER_CACHE:
					setCacheText(Storable.getString(fld.data));
					break;
				case IOConstants.TEASER_PREVIEW:
					setPreview(Storable.getString(fld.data));
					break;
			}
		}
	}

	public void toNVs(List<NV> nvs) throws ApplicationFault {

		if ( null != this.id ) nvs.add( new NV(
				IOConstants.TEASER_BYTES,IOConstants.TEASER_ID_BYTES, this.id ) );
		
		if ( null != this.url) {
			String encodedUrl = UrlMapper.getInstance().encoding(
				(String) this.url.getValue());
			nvs.add( new NV(IOConstants.TEASER_BYTES,IOConstants.TEASER_URL_BYTES, new Storable(encodedUrl)));
		}
		
		if ( null != this.title ) nvs.add( new NV(
			IOConstants.TEASER_BYTES,IOConstants.TEASER_TITLE_BYTES, this.title ) );

		if ( null != this.cacheText ) nvs.add( new NV(
			IOConstants.TEASER_BYTES,IOConstants.TEASER_CACHE_BYTES, this.cacheText ));

		if ( null != this.preview ) nvs.add( new NV(
				IOConstants.TEASER_BYTES,IOConstants.TEASER_PREVIEW_BYTES, this.preview ));
	}
	
	public void setUrl(String url) {
		if ( null == url) this.url = null;
		else this.url = new Storable(url);
	}
	
	public String getUrl() {
		if ( null != this.url) return (String)  url.getValue();
		return null;
	}
	
	public void setCacheText(String bodyText) {
		if ( null == bodyText) this.cacheText = null;
		else this.cacheText = new Storable(bodyText);
	}
	
	public String getCachedText() {
		if ( null != this.cacheText) return (String) cacheText.getValue();
		return null;
	}
	
	public void setPreview(String preview) {
		if ( null == preview) this.preview = null;
		else this.preview = new Storable(preview); 
	}
	
	public String getPreview() {
		if ( null != this.preview) return (String) preview.getValue();
		return null;
	}

	public void setTitle(String title) {
		if ( null == title) this.title = null;
		else this.title = new Storable(title); 
	}
	
	public String getTitle() {
		if ( null != this.title) return (String) title.getValue();
		return null;
	}

	/**
	 * Clean up the entire set.
	 */
	public void cleanup() {
		this.id =  null;
		this.url =  null;
		this.title =  null; 
		this.cacheText =  null;
		this.preview =  null;
	}
	
	public void toXml(Writer pw) throws IOException {
		if ( null == pw) return;
		pw.append('<');
		pw.append(IOConstants.TEASER);
		pw.append('>');
		if ( null != id ) {
			pw.append('<').append(IOConstants.TEASER_ID).append('>');
			pw.append( new String(id.toBytes()) );
			pw.append('<').append(IOConstants.TEASER_ID).append("/>");
		}
		
		if ( null != url ) {
			pw.append('<').append(IOConstants.TEASER_URL).append('>');
			pw.append( url.toString() );
			pw.append('<').append(IOConstants.TEASER_URL).append("/>");
		}
		
		if ( null != title ) {
			pw.append('<').append(IOConstants.TEASER_TITLE).append('>');
			pw.append( title.toString() );
			pw.append('<').append(IOConstants.TEASER_TITLE).append("/>");
		}

		if ( null != cacheText ) {
			pw.append('<').append(IOConstants.TEASER_CACHE).append('>');
			pw.append( cacheText.toString() );
			pw.append('<').append(IOConstants.TEASER_CACHE).append("/>");
		}

		if ( null != preview ) {
			pw.append('<').append(IOConstants.TEASER_PREVIEW).append('>');
			pw.append( preview.toString() );
			pw.append('<').append(IOConstants.TEASER_PREVIEW).append("/>");
		}
		
		pw.append("</").append(IOConstants.TEASER).append(">");
	}
	
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder('\n');
		if ( null != mappedId ) sb.append("Mapped Id : [").append(mappedId);
		if ( null != id ) sb.append("Id : [").append(new String(id.toBytes()));
		if ( null != url ) sb.append("] , Url : [").append(url.toString());
		if ( null != title ) sb.append("] , Title : [").append(title.toString());
		if ( null != cacheText ) sb.append("] , Body :[").append(cacheText.toString());
		if ( null != preview ) sb.append("] , Preview :[").append(preview.toString());
		sb.append(']');
		return sb.toString();
	}
}
