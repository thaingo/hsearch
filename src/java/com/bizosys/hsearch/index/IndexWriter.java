package com.bizosys.hsearch.index;

import java.util.ArrayList;
import java.util.List;

import org.apache.oneline.ApplicationFault;
import org.apache.oneline.SystemFault;
import org.apache.oneline.pipes.PipeIn;

import com.bizosys.hsearch.common.HDocument;

/**
 * 
 * @author karan
 *
 */
public class IndexWriter {

	/**
	 * Insert only one document
	 * @param hdoc
	 * @param pipes
	 * @param docMergeFactor
	 * @throws ApplicationFault
	 * @throws SystemFault
	 */
	public void insert(HDocument hdoc, List<PipeIn> pipes, 
		int docMergeFactor) throws ApplicationFault, SystemFault{
		
		Doc doc = new Doc(hdoc);
		L.l.info("Insert Step 1 > Value parsing is over.");

		for (PipeIn in : pipes) {
			in.visit(doc);
		}
		L.l.info("Insert Step 2 >  Pipe processing is over.");
		
		for (PipeIn in : pipes) {
			in.commit();
		}
		L.l.info("Insert Step 3 >  Commit is over.");
		
	}
	
	public void insert(List<HDocument> hdocs, List<PipeIn> pipes, int mergeFactor) throws ApplicationFault, SystemFault{
		if ( null == hdocs) return;
		
		List<Doc> docs = new ArrayList<Doc>(hdocs.size());
		for (HDocument hdoc : hdocs) {
			Doc doc = new Doc(hdoc);
			docs.add(doc);
		}
		L.l.info("Insert Step 1 > Value parsing is over.");
		
		for (Doc doc : docs) {
			for (PipeIn in : pipes) {
				in.visit(doc);
			}
		}
		L.l.info("Insert Step 2 >  Pipe processing is over.");
		
		for (PipeIn in : pipes) {
			in.commit();
		}
		L.l.info("Insert Step 3 >  Commit is over.");
	}
}
