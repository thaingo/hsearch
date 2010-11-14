package com.bizosys.hsearch.index;

import com.bizosys.hsearch.common.HDocument;

public class Doc {
	
	public DocTerms terms = null;
	public DocMeta meta = null;
	public DocAcl acl = null;
	public DocTeaser teaser = null;
	public DocContent content = null;
	
	public String ipAddress = null;
	public Long bucketId = null;
	public Short docSerialId = null;

	public Doc() {
	}
	
	public Doc(HDocument hDoc) {
		this.bucketId = hDoc.bucketId;
		this.docSerialId = hDoc.docSerialId;
		this.ipAddress = hDoc.ipAddress;
		
		this.meta = new DocMeta(hDoc);
		this.teaser = new DocTeaser(hDoc);
		this.content = new DocContent(hDoc);
		this.acl = new DocAcl(hDoc);
		this.terms = new DocTerms();
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
		sb.append("\n Doc Serial :").append(docSerialId);
		sb.append("\n Term :").append(terms.toString());
		sb.append("\n Acl : ").append(acl.toString());
		sb.append("\n Meta :").append(meta.toString());
		sb.append("\n Teaser:").append(teaser.toString());
		sb.append("\n Content").append(content.toString());
		sb.append(">>>> Document Ends <<<<\n");
		return sb.toString();
	}
}
