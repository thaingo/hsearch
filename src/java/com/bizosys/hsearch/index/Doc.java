package com.bizosys.hsearch.index;

import java.util.List;

import com.bizosys.hsearch.common.HDocument;
import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.hbase.HReader;
import com.bizosys.hsearch.hbase.NVBytes;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;

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
		this.content = new DocContent(contentB );
		contentB.clear();
		
		/**
		 * Get the Meta
		 */
		List<NVBytes> previewB = HReader.getCompleteRow(
			IOConstants.TABLE_PREVIEW, mappedKey.getBytes());
		this.teaser = new DocTeaser(origId.getBytes(), previewB);
		for (NVBytes nv : previewB) {
			if ( Storable.compareBytes(nv.name, IOConstants.META_BYTES)) 
				this.meta = new DocMeta(nv.data);
			else if ( Storable.compareBytes(nv.name, IOConstants.ACL_BYTES))
				this.acl = new DocAcl(nv.data);
		}
		previewB.clear();
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
}
