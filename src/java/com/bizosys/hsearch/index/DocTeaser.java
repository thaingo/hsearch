package com.bizosys.hsearch.index;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.bizosys.oneline.ApplicationFault;

import com.bizosys.hsearch.common.ByteField;
import com.bizosys.hsearch.common.HDocument;
import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.hbase.NVBytes;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.hsearch.util.UrlMapper;

/**
 * Documents could be coming as structured or unstructured.
 * @author bizosys
 *
 */
public class DocTeaser {

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

	/**
	 * All the calculated resolved terms out from regular texts
	 */
	public Map<String, String> resolvedTerms = null;
	
	/**
	 * This is fresh content which has been given to 
	 * the search engine instead of a body text. 
	 */
	protected Map<String, ByteField[]> structuredFields = null;
	
	public DocTeaser() {
	}
	
	public DocTeaser(HDocument aDoc) {
		this.id = aDoc.originalId;
		this.url = aDoc.url;
		this.title =  aDoc.title;
		this.preview =  aDoc.preview;
	}
	
	public DocTeaser (byte[] id, List<NVBytes> inputBytes) throws ApplicationFault {
		this.id = new Storable(id, Storable.BYTE_STRING);
		for (NVBytes fld : inputBytes) {
			switch(fld.name[0]) {
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
	 * Here resolved terms are maintained.
	 * Example [Human Rights] =  dc:rights
	 * @return
	 */
	public Map<String, String> getResolvedTerms() {
		if ( null == this.resolvedTerms) 
			this.resolvedTerms = new HashMap<String, String>();
		return resolvedTerms;
	}

	public void setResolvedTerms(Map<String, String> resolved) {
		if ( null == this.resolvedTerms) {
			this.resolvedTerms = resolved;
		} else {
			this.resolvedTerms.putAll(resolved);
		}
	}
	
	/**
	 * Here structured terms are maintained.
	 * Example [dc:rights]   =   [ByteField Name:dc:rights , Value = Human Rights]
	 * @return
	 */
	public Map<String, ByteField[]> getStructuredFields() {
		return this.structuredFields;
	}

	public void addStructuredField(ByteField param) {
		if ( null == this.structuredFields) {
			this.structuredFields = new HashMap<String, ByteField[]>();
			this.structuredFields.put(param.name, new ByteField[] {param});
		} else {
			if ( this.structuredFields.containsKey(param.name)) {
				ByteField[] bfL = this.structuredFields.get(param.name);
				
				int bfT = bfL.length;
				ByteField[] expaned =	new ByteField[bfT + 1];
				System.arraycopy(bfL, 0, expaned, 0, bfT);
				Arrays.fill(bfL, null);
				bfL = null;
				expaned[bfT] = param;
				this.structuredFields.put(param.name, expaned);				
			} else {
				this.structuredFields.put(param.name, new ByteField[] {param});				
			}
		}
	}
	
	/**
	 * Clean up the entire set.
	 */
	public void cleanup() {
		this.url =  null;
		this.title =  null; 
		this.cacheText =  null;
		this.preview =  null;
		
		if ( null != resolvedTerms) {
			this.resolvedTerms.clear();
			this.resolvedTerms = null;
		}
		if ( null != structuredFields) {
			this.structuredFields.clear();
			this.structuredFields = null;
		}
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder('\n');
		if ( null != url ) sb.append("Url :").append(url);
		if ( null != title ) sb.append("\nTitle :").append(title);
		if ( null != cacheText ) sb.append("\nBody :").append(cacheText);
		if ( null != preview ) sb.append("\nPreview :").append(preview);
		if ( null != this.structuredFields) {
			sb.append("StructuredFields :\n");
			byte[] valObj = null;
			for ( String key : this.structuredFields.keySet()) {
				ByteField[] storable = this.structuredFields.get(key);
				if ( (null != storable) ) {
					for (ByteField field : storable) {
						valObj = field.toBytes();
						if ( null == valObj ) continue;
						sb.append(field.name).append('=').append( new String(valObj) );
						sb.append('\n');
					}
				}
			}
		}
		
		return sb.toString();
	}
}
