package com.bizosys.hsearch.inpipe;

import java.util.ArrayList;
import java.util.List;

import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.hbase.HWriter;
import com.bizosys.hsearch.hbase.IUpdatePipe;
import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.schema.ILanguageMap;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;

public class DeleteFromIndex implements PipeIn {

	List<Doc> documents = new ArrayList<Doc>();
	
	public DeleteFromIndex() {
	}
	
	public DeleteFromIndex(int docMergeFactor) {
	}

	public boolean visit(Object objDoc) throws ApplicationFault, SystemFault {
		
		return true;
	}

	/**
	 * Cuts out section of docpositions which are in the removal list.
	 */
	public boolean commit() throws ApplicationFault, SystemFault {
		Doc curDoc = null; 
		try {
			for (Doc aDoc : documents) {
				curDoc = aDoc;
				IUpdatePipe pipe = new DeleteFromIndexWithCut(aDoc.docSerialId);
				byte[] pk = Storable.putLong(aDoc.bucketId);
				for (Character c : ILanguageMap.ALL_TABLES) {
					String t = c.toString();
					HWriter.update(t, pk, pipe);
				}
			}
			return true;
		} catch (Exception ex) {
			if ( null != curDoc) throw new SystemFault(curDoc.toString(), ex);
			else throw new SystemFault(ex);
		}
	}

	public boolean init(Configuration conf) throws ApplicationFault, SystemFault {
		return true;
	}

	public PipeIn getInstance() {
		return this;
	}

	public String getName() {
		return "DeleteFromIndex";
	}
}
