package com.bizosys.hsearch.index;

import java.util.List;

import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.pipes.PipeOut;

import com.bizosys.hsearch.common.MappedDocId;

/**
 * 
 * @author karan
 *
 */
public class IndexReader {

	/**
	 * Raw index reading to find the matching documents.
	 * Based on this Id, we will read the document body section to show
	 * @param singleTerm
	 * @param pipes
	 * @return
	 * @throws ApplicationFault
	 * @throws SystemFault
	 */
	public List<MappedDocId> find(String singleTerm, List<PipeOut> pipes) throws ApplicationFault, SystemFault{
		return null;
	}
}
