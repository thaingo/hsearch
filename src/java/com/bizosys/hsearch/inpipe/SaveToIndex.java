package com.bizosys.hsearch.inpipe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.oneline.ApplicationFault;
import org.apache.oneline.SystemFault;
import org.apache.oneline.conf.Configuration;
import org.apache.oneline.pipes.PipeIn;

import com.bizosys.hsearch.common.IStorable;
import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.index.BucketIsFullException;
import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.IdMapping;
import com.bizosys.hsearch.index.L;
import com.bizosys.hsearch.index.Term;
import com.bizosys.hsearch.index.TermColumns;
import com.bizosys.hsearch.index.TermFamilies;
import com.bizosys.hsearch.index.TermTables;
import com.bizosys.hsearch.schema.ILanguageMap;
import com.bizosys.hsearch.schema.SchemaManager;
import com.bizosys.hsearch.util.RecordScalar;

public class SaveToIndex implements PipeIn {

	int docMergeFactor = 10000;
	/** Arranged by document */
	Map<Doc, TermTables> docTermTables = new HashMap<Doc, TermTables>();
	
	public SaveToIndex() {
		
	}
	
	public SaveToIndex(int docMergeFactor) {
		this.docMergeFactor = docMergeFactor;
	}

	public boolean visit(Object objDoc) throws ApplicationFault, SystemFault {
		
		if ( null == objDoc) return true;
		Doc doc = (Doc) objDoc;

		if ( null == doc.terms) return true;
		if ( null == doc.terms.all) return true;
		
		ILanguageMap map = SchemaManager.getInstance().getLanguageMap(doc.meta.locale);
		TermTables termTable = ( null == doc.bucketId ) ?
			new TermTables() : new TermTables( new Storable(doc.bucketId));
			
		for (Term term : doc.terms.all) {
			termTable.add(term, map);
		}
		this.docTermTables.put(doc, termTable);
		return true;
	}

	/**
	 * Creating the term bucket to save the changes.
	 */
	public boolean commit() throws ApplicationFault, SystemFault {

		if ( null == this.docTermTables) return true;
		
		/**
		 * We need to arrange all terms from documents to arrange in term buckets.
		 */
		Map<Long, TermTables> mergedTermTables = new HashMap<Long, TermTables>();
		int totalDocsCount = this.docTermTables.size();
		int updateDocsCount = 0;
		
		/**
		 * Existing Document : Look for existing documents with valid bucket Id 
		 */
		for (TermTables docTermTable : this.docTermTables.values()) {
			if ( null == docTermTable.bucketId ) continue; //New Record
			updateDocsCount++;
			buildTermTables(mergedTermTables, docTermTable);
		}
		
		/**
		 * New records - Generate Keys for the bucket and documents
		 */
		int newDocsCount = totalDocsCount - updateDocsCount;
		
		long currentBucket =  -1;
		short docPos = Short.MIN_VALUE;
		try {
			currentBucket =  TermTables.getCurrentBucketId();
			docPos = TermTables.createDocumentSerialIds(
				currentBucket,newDocsCount);
			
			if ( L.l.isInfoEnabled()) L.l.info("StoreToIndex > Document Serial Position moved till :" + docPos);
			if ( docPos > docMergeFactor ) TermTables.createBucketId();
			
		} catch (BucketIsFullException ex) {
			throw new ApplicationFault("StoreToIndex : Reduce the merge Factor. It is beyond the short data range.", ex);
		}
		
		/**
		 * Assign the created bucketId and coument position to new docs
		 * Create a Key Map with the original Ids
		 */
		IStorable storableBucketId = new Storable(currentBucket);
		
		List<IdMapping> docMappedIds = null;
		if (newDocsCount > 0 ) docMappedIds = new ArrayList<IdMapping>(newDocsCount);
		
		for ( Doc doc: this.docTermTables.keySet()) {

			TermTables docTermTable = this.docTermTables.get(doc);
			if ( null != docTermTable.bucketId ) continue;
			
			//Assign Id and Serial Position
			docTermTable.bucketId = storableBucketId;
			short thisDocPosition = docPos--;
			docTermTable.assignDocumentPosition(thisDocPosition);
			
			//Set bucket if and doc serial id for original document.
			doc.bucketId = Storable.getLong(0, storableBucketId.toBytes());
			doc.docSerialId = thisDocPosition;
			
			//Store the mapping 
			docMappedIds.add(new IdMapping(
				doc.teaser.id,currentBucket,thisDocPosition));
			
			//Dedup Terms
			buildTermTables(mergedTermTables, docTermTable);
		}
		
		if ( L.l.isDebugEnabled()) L.l.debug(printMergedTT(mergedTermTables));

		/**
		 * Persist Ids
		 */
		if ( null != docMappedIds) {
			List<RecordScalar> mapRecords = new ArrayList<RecordScalar>(docMappedIds.size()); 
			for (IdMapping mapping : docMappedIds) {
				mapping.build(mapRecords);
			}
			IdMapping.persist(mapRecords);
		}

		/**
		 * Persist Terms
		 */
		if ( 0 == mergedTermTables.size()) return true;
		for (Long bucketId : mergedTermTables.keySet()) {
			mergedTermTables.get(bucketId).persist(true);
		}
		
		return true;
	}



	/**
	 * 
	 * @param mergedTermTables
	 * @param docTermTable
	 */
	private void buildTermTables(
		Map<Long, TermTables> mergedTermTables, TermTables docTermTable) {
		
		byte[] bucketIdB = docTermTable.bucketId.toBytes();
		long bucketId = Storable.getLong(0, bucketIdB);
		
		if ( mergedTermTables.containsKey(bucketId)) {
			TermTables mtt = mergedTermTables.get(bucketId);
			mtt.add(docTermTable);
		} else {
			mergedTermTables.put(bucketId, docTermTable);
		}
	}

	public boolean init(Configuration conf) throws ApplicationFault, SystemFault {
		this.docMergeFactor = 
			conf.getInt("index.documents.merge", 10000);
		return true;
	}

	public PipeIn getInstance() {
		return new SaveToIndex(this.docMergeFactor);
	}

	public String getName() {
		return "SaveToIndex";
	}
	
	/**
	 * Creates a string representation of the table.
	 * @param mergedTermTables
	 * @return
	 */
	private String  printMergedTT(Map<Long, TermTables> mergedTermTables) {
		StringBuilder sb = new StringBuilder();
		for (long bucket : mergedTermTables.keySet()) {
			sb.append("Bucket:").append(bucket);
			TermTables tt =  mergedTermTables.get(bucket);
			for (char table: tt.tables.keySet()) {
				sb.append("\n\tTable:").append(table);
				TermFamilies tf = tt.tables.get(table);
				for (char family : tf.families.keySet()) {
					sb.append("\n\t\tfamily:").append(family);
					TermColumns tc = tf.families.get(family);
					for (char col : tc.columns.keySet()) {
						sb.append("\n\t\t\tColumn:").append(col);
						sb.append(tc.columns.get(col).toString());
					}
				}
			}
		}
		return sb.toString();
	}	

}
