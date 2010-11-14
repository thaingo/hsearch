package com.bizosys.hsearch.inpipe;

import java.util.ArrayList;
import java.util.List;

import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;

import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.hbase.HWriter;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.IdMapping;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.hsearch.util.Record;

public class SaveToPreview implements PipeIn {

	List<Doc> previews = new ArrayList<Doc>();
	public SaveToPreview() {
		
	}
	
	public boolean visit(Object objDoc) throws ApplicationFault, SystemFault {
		
		if ( null == objDoc) return true;
		Doc doc = (Doc) objDoc;

		if ( null == doc.content) return true;
		
		if ( null == previews) previews = new ArrayList<Doc>();
		this.previews.add(doc);
		return true;
	}

	/**
	 * Creating the term bucket to save the changes.
	 */
	public boolean commit() throws ApplicationFault, SystemFault {

		if ( null == this.previews) return true;
		
		/**
		 * Iterate through all content 
		 */
		try {
			List<Record> previewRecords = new ArrayList<Record>(this.previews.size()); 
			for (Doc doc : this.previews) {
				if ( null == doc.content ) continue;
				String id = IdMapping.getKey(doc.bucketId, doc.docSerialId);
				List<NV> nvs = new ArrayList<NV>();
				doc.meta.toNVs(nvs);
				doc.acl.toNVs(nvs);
				doc.teaser.toNVs(nvs);
				previewRecords.add(new Record(new Storable(id),nvs));
			}
			HWriter.insert(IOConstants.TABLE_CONTENT, previewRecords, true);
		} catch (Exception ex) {
			throw new ApplicationFault("SaveToPreview : Failed.", ex);
		}
		return true;
	}

	public boolean init(Configuration conf) throws ApplicationFault, SystemFault {
		return true;
	}

	public PipeIn getInstance() {
		return new SaveToPreview();
	}

	public String getName() {
		return "SaveToPreview";
	}

}
