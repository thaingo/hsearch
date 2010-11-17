package com.bizosys.hsearch.index;

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
	 * Clean up the entire set.
	 */
	public void cleanup() {
		this.id =  null;
		this.url =  null;
		this.title =  null; 
		this.cacheText =  null;
		this.preview =  null;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder('\n');
		if ( null != id ) sb.append("Id :").append(id);
		if ( null != url ) sb.append("Url :").append(url);
		if ( null != title ) sb.append("\nTitle :").append(title);
		if ( null != cacheText ) sb.append("\nBody :").append(cacheText);
		if ( null != preview ) sb.append("\nPreview :").append(preview);
		return sb.toString();
	}
}
