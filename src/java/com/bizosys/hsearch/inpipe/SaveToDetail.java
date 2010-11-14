package com.bizosys.hsearch.inpipe;

import java.util.ArrayList;
import java.util.List;

import org.apache.oneline.ApplicationFault;
import org.apache.oneline.SystemFault;
import org.apache.oneline.conf.Configuration;
import org.apache.oneline.pipes.PipeIn;

import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.hbase.HWriter;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.IdMapping;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.hsearch.util.Record;

public class SaveToDetail implements PipeIn {

	List<Doc> details = new ArrayList<Doc>();
	public SaveToDetail() {
		
	}
	
	public boolean visit(Object objDoc) throws ApplicationFault, SystemFault {
		
		if ( null == objDoc) return true;
		Doc doc = (Doc) objDoc;

		if ( null == doc.content) return true;
		
		if ( null == details) details = new ArrayList<Doc>();
		this.details.add(doc);
		return true;
	}

	/**
	 * Creating the term bucket to save the changes.
	 */
	public boolean commit() throws ApplicationFault, SystemFault {

		if ( null == this.details) return true;
		
		/**
		 * Iterate through all content 
		 */
		try {
			List<Record> contentRecords = new ArrayList<Record>(this.details.size()); 
			for (Doc doc : this.details) {
				if ( null == doc.content ) continue;
				String id = IdMapping.getKey(doc.bucketId, doc.docSerialId);
				List<NV> nvs = new ArrayList<NV>();
				doc.content.toNVs(nvs);
				contentRecords.add(new Record(new Storable(id),nvs));
			}
			HWriter.insert(IOConstants.TABLE_CONTENT, contentRecords, true);
		} catch (Exception ex) {
			throw new ApplicationFault("SaveToDetail : Failed", ex);
		}
		return true;
	}

	public boolean init(Configuration conf) throws ApplicationFault, SystemFault {
		return true;
	}

	public PipeIn getInstance() {
		return new SaveToDetail();
	}

	public String getName() {
		return "SaveToDetail";
	}

}
