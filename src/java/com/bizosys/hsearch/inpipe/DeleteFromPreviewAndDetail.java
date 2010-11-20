package com.bizosys.hsearch.inpipe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.bizosys.hsearch.hbase.HWriter;
import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.IdMapping;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;

public class DeleteFromPreviewAndDetail implements PipeIn {

	List<byte[]> ids = new ArrayList<byte[]>(3);
	public DeleteFromPreviewAndDetail() {
		
	}
	
	public boolean visit(Object objDoc) throws ApplicationFault, SystemFault {
		
		if ( null == objDoc) return true;
		Doc doc = (Doc) objDoc;
		
		if ( null != doc.bucketId && null != doc.docSerialId ) {
			ids.add(IdMapping.getKey(doc.bucketId, doc.docSerialId).getBytes());
			return true;
		} else {
			L.l.warn("DeleteFromPreviewAndDetail: Document Original Id or Bucket Id with doc serial is absent.");
			throw new ApplicationFault ("DeleteFromPreviewAndDetail: No Ids provided for deletion.");
		}
	}

	/**
	 * Creating the term bucket to save the changes.
	 */
	public boolean commit() throws ApplicationFault, SystemFault {

		if ( 0 == this.ids.size()) return true;

		try {
			HWriter.delete(IOConstants.TABLE_CONTENT, ids);
			HWriter.delete(IOConstants.TABLE_PREVIEW, ids);
		} catch (IOException ex) {
			StringBuilder sb = new StringBuilder(100);
			sb.append("DeleteFromPreviewAndDetail: Failed Ids are = ");
			for (byte[] id : this.ids) {
				sb.append('[').append(new String(id)).append("] ");
			}
			throw new SystemFault(sb.toString(), ex);
		}
		return true;
	}

	public boolean init(Configuration conf) throws ApplicationFault, SystemFault {
		return true;
	}

	public PipeIn getInstance() {
		return new DeleteFromPreviewAndDetail();
	}

	public String getName() {
		return "DeleteFromPreviewAndDetail";
	}

}
