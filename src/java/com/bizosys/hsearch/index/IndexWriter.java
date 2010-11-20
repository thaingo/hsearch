package com.bizosys.hsearch.index;

import java.util.ArrayList;
import java.util.List;

import com.bizosys.hsearch.common.HDocument;
import com.bizosys.hsearch.inpipe.ComputeTokens;
import com.bizosys.hsearch.inpipe.DeleteFromDictionary;
import com.bizosys.hsearch.inpipe.DeleteFromIndex;
import com.bizosys.hsearch.inpipe.DeleteFromPreviewAndDetail;
import com.bizosys.hsearch.inpipe.FilterLowercase;
import com.bizosys.hsearch.inpipe.FilterStem;
import com.bizosys.hsearch.inpipe.FilterStopwords;
import com.bizosys.hsearch.inpipe.FilterTermLength;
import com.bizosys.hsearch.inpipe.SaveToDetail;
import com.bizosys.hsearch.inpipe.SaveToDictionary;
import com.bizosys.hsearch.inpipe.SaveToIndex;
import com.bizosys.hsearch.inpipe.SaveToPreview;
import com.bizosys.hsearch.inpipe.TokenizeStandard;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.pipes.PipeIn;

/**
 * 
 * @author karan
 *
 */
public class IndexWriter {

	private static IndexWriter singleton = null;
	public static IndexWriter getInstance() {
		if ( null != singleton) return singleton;
		synchronized (IndexWriter.class) {
			if ( null != singleton) return singleton;
			singleton = new IndexWriter();
		}
		return singleton;
	}
	
	private List<PipeIn> standardPipes = null;

	private IndexWriter() {
		this.standardPipes = new ArrayList<PipeIn>();
		
		this.standardPipes.add(new TokenizeStandard());
		this.standardPipes.add(new FilterStopwords());
		this.standardPipes.add(new FilterTermLength());
		this.standardPipes.add(new FilterLowercase());
		this.standardPipes.add(new FilterStem());
		this.standardPipes.add(new ComputeTokens());
		this.standardPipes.add(new SaveToIndex());
		this.standardPipes.add(new SaveToDictionary());
		this.standardPipes.add(new SaveToPreview());
		this.standardPipes.add(new SaveToDetail());
	}
	
	/**
	 * If necessary create copies and keep
	 * @return
	 */
	public List<PipeIn> getStandardPipes() {
		List<PipeIn> pipes = new ArrayList<PipeIn>(this.standardPipes.size());
		for (PipeIn spipe : this.standardPipes) {
			pipes.add(spipe.getInstance());
		}
		return pipes;
	}
	
	public void setStandardPipes(List<PipeIn> pipes) {
		this.standardPipes = pipes;
	}
	
	public void insert(HDocument hdoc) throws ApplicationFault, SystemFault{
		List<PipeIn> localPipes = getStandardPipes();
		insert(hdoc,localPipes);
	}
	
	public void insert(HDocument hdoc, List<PipeIn> localPipes) throws ApplicationFault, SystemFault{
		
		Doc doc = new Doc(hdoc);
		L.l.info("Insert Step 1 > Value parsing is over.");
		
		for (PipeIn in : localPipes) {
			in.visit(doc);
		}
		L.l.info("Insert Step 2 >  Pipe processing is over.");
		
		for (PipeIn in : standardPipes) {
			in.commit();
		}
		L.l.info("Insert Step 3 >  Commit is over.");

	}

	public void insert(List<HDocument> hdocs) throws ApplicationFault, SystemFault{
		List<PipeIn> localPipes = getStandardPipes();
		insert(hdocs,localPipes);
	}
	
	public void insert(List<HDocument> hdocs, List<PipeIn> pipes) throws ApplicationFault, SystemFault{
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
	
	/**
	 * 1 : Load the original document
	 * 2 : Parse the document 
	 * 2 : Remove From Dictionry, Index, Preview and Detail  
	 */
	public void delete(String documentId) throws ApplicationFault, SystemFault {
		Doc origDoc = IndexReader.getInstance().get(documentId);

		List<PipeIn> deletePipe = new ArrayList<PipeIn>();
		
		deletePipe.add(new TokenizeStandard());
		deletePipe.add(new FilterStopwords());
		deletePipe.add(new FilterTermLength());
		deletePipe.add(new FilterLowercase());
		deletePipe.add(new FilterStem());
		deletePipe.add(new ComputeTokens());

		deletePipe.add(new DeleteFromIndex());
		deletePipe.add(new DeleteFromPreviewAndDetail());
		deletePipe.add(new DeleteFromDictionary());
		
		L.l.info("Delete Step 1 > Value parsing is over.");
		
		for (PipeIn in : deletePipe) {
			in.visit(origDoc);
		}
		
		L.l.info("Delete Step 2 >  Pipe processing is over.");
		for (PipeIn in : deletePipe) {
			in.commit();
		}
		L.l.info("Insert Step 3 >  Commit is over.");
	}
}
