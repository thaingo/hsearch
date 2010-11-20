package com.bizosys.hsearch.index;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.bizosys.hsearch.common.Storable;

public class InvertedIndex {
	public int hash;
	public byte[] dtc; //doc type code
	public byte[] ttc; //term type code
	public byte[] tw; //term weight
	public byte[] termFreq; //term freq
	public short[] termPos; //term pos
	public short[] docPos; //doc pos
	
	public InvertedIndex(int hash, byte[] dtc, byte[] ttc, byte[] tw,  
		byte[] termFreq,short[] termPos,short[] docPos ) {
		
		this.hash = hash;
		this.dtc = dtc;
		this.ttc = ttc;
		this.tw = tw;
		this.termFreq = termFreq;
		this.termPos = termPos;
		this.docPos = docPos;
	}
	

	
	/**
	 * Reads the bytes to reconstruct the Inverted Index
	 * @param bytes
	 * @return
	 */
	public static List<InvertedIndex> read(byte[] bytes) {
		
		if ( null == bytes) return null;
		int pos = 0;
		int bytesT = bytes.length;
		if ( 0 == bytesT) return null;
		
		List<InvertedIndex> invIndex = new ArrayList<InvertedIndex>(); 
		
		while (pos < bytesT) {
			int hash = Storable.getInt(pos, bytes);
			pos = pos + 4;
			int termsT = (byte) bytes[pos++];
			if ( -1 == termsT) {
				termsT = Storable.getInt(pos,bytes );
				pos = pos + 4;
			}

			byte[] dtc = new  byte[termsT];
			System.arraycopy(bytes, pos, dtc, 0, termsT);
			pos = pos + termsT;
			
			byte[] ttc = new  byte[termsT];
			System.arraycopy(bytes, pos, ttc, 0, termsT);
			pos = pos + termsT;
			
			byte[] tw = new  byte[termsT];
			System.arraycopy(bytes, pos, tw, 0, termsT);
			pos = pos + termsT;
			
			byte[] tf = null;
			short[] tp = null;
			if ( TermList.termVectorStorageEnabled ) {
				tf = new  byte[termsT];
				System.arraycopy(bytes, pos, tf, 0, termsT);
				pos = pos + termsT;
		
				tp = new  short[termsT];
				for (int i=0; i< termsT; i++) {
					tp[i] = Storable.getShort(pos, bytes);
					pos = pos + 2;
				}
			}

			short[]  dp = new  short[termsT];
			for (int i=0; i< termsT; i++) {
				dp[i] = Storable.getShort(pos, bytes);
				pos = pos + 2;
			}
			
			InvertedIndex ii = new InvertedIndex(hash,dtc, ttc,tw,tf,tp,dp);
			invIndex.add(ii);
		}
		return invIndex;
	}
	
	/**
	 * Remove the document at the specified position
	 * @param bytes
	 * @return
	 */
	public static byte[] delete(byte[] bytes, short docPos) {
		
		if ( null == bytes) return null;
		int pos = 0;
		int bytesT = bytes.length;
		if ( 0 == bytesT) return null;
		
		Map<Integer,Integer> rowcol = new HashMap<Integer,Integer>(); 
		int row = 0;
		int termsT = 0;
		int col = -1;
		short dp;
		while (pos < bytesT) {
			row++;
			pos = pos + 4;
			termsT = (byte) bytes[pos++];
			if ( -1 == termsT) {
				termsT = Storable.getInt(pos,bytes );
				pos = pos + 4;
			}
			
			pos = pos + (termsT * 3); //dtc + ttc + tw
			if ( TermList.termVectorStorageEnabled ) pos = pos + (termsT * 3); //tf + tp
			col = -2;
			for (int i=0; i< termsT; i++) {
				dp = Storable.getShort(pos, bytes);
				pos = pos + 2;
				if ( dp == docPos) {
					col = ( termsT == 1 ) ? -1 : i; 
					break;
				}
			}
			if ( -2 != col ){
				rowcol.put(row,col);
				pos = pos + (termsT - col - 1) * 2;
			}
		}
		
		/**
		 * Now cut the actual values
		 */
		pos = 0; row = 0;
		ByteBuffer bb = ByteBuffer.allocate(bytes.length);
		
		while (pos < bytesT) {
			row++;
			boolean cutRow = rowcol.containsKey(row);
			if ( cutRow && rowcol.get(row) == -1 ) {
				pos = pos + 4;
				termsT = (byte) bytes[pos++];
				if ( -1 == termsT) termsT = Storable.getInt(pos,bytes );
				pos = pos + 4;
				if ( TermList.termVectorStorageEnabled ) pos = pos + termsT * 8; 
				else pos = pos + termsT * 5;
				continue;
			}
			
			bb.put(bytes, pos, 4);
			pos = pos + 4;
			termsT = (byte) bytes[pos++];
			if ( -1 == termsT) {
				bb.put( (byte) -1);
				termsT = Storable.getInt(pos,bytes );
				bb.put(bytes, pos, 4);
				pos = pos + 4;
			} else {
				if ( cutRow ) bb.put( (byte) (termsT - 1) );
				else bb.put( (byte) (termsT) );
			}
			
			if ( cutRow ) {
				col = rowcol.get(row);
				if ( col != 0 ) bb.put(bytes, pos, col);
				bb.put(bytes, pos + col + 1, termsT - col - 1);
				pos = pos + termsT;

				//Copy Term Type Code
				if ( col != 0 )  bb.put(bytes, pos, col);
				bb.put(bytes, pos + col + 1, termsT - col - 1);
				pos = pos + termsT;

				//Copy Term Weight
				if ( col != 0 ) bb.put(bytes, pos, col);
				bb.put(bytes, pos + col + 1, termsT - col - 1);
				pos = pos + termsT;

				if ( TermList.termVectorStorageEnabled ) {
					//Copy Term Frequency
					if ( col != 0 ) bb.put(bytes, pos, col);
					bb.put(bytes, pos + col + 1, termsT - col - 1);
					pos = pos + termsT;
					
					//Copy Term Position
					if ( col != 0 ) bb.put(bytes, pos, (col) * 2 );
					bb.put(bytes, pos + (col + 1) * 2, (termsT - col - 1) * 2);
					pos = pos + termsT * 2;
				} 
				//Copy Doc Position
				if ( col != 0 ) bb.put(bytes, pos, col * 2 );
				bb.put(bytes, pos + (col + 1) * 2, (termsT - col - 1) * 2);
				pos = pos + termsT * 2;
				
			} else {
				if ( TermList.termVectorStorageEnabled ) {
					bb.put(bytes, pos, termsT * 8);
					pos = pos + termsT * 8; 
				} else {
					bb.put(bytes, pos, termsT * 5);
					pos = pos + termsT * 5; 
				}
			}
		}
		int len = bb.position();
		if ( 0 == len ) return null;
		byte[] deletedB = new byte[len];
		bb.position(0);
		bb.get(deletedB, 0, len);
		bb.clear();
		return deletedB;
	}	
		
	
	/**
	 * Merge the supplied document list with the documents
	 * already present in the bucket.
	 * 
	 * Ignore all the supplied documents while loading from bytes the existing ones
	 * Create the Term List 
	 *
	 */
	public static void merge(byte[] existingB, 
			Map<Integer, List<Term>> lstKeywords) {
		if ( null == existingB) return;

		short docPos;
		Set<Short> freshDocs = getFreshDocs(lstKeywords);
		
		int bytesT = existingB.length;
		List<Term> priorDocTerms = new ArrayList<Term>();
		int keywordHash = -1, termsT = -1, shift = 0, pos = 0, readPos=0;
		byte docTyep=0,termTyep=0,termWeight=0,termFreq=0;
		short termPos=0;
		
		while ( pos < bytesT) {
			if ( L.l.isDebugEnabled() ) 
				L.l.debug("TermList Byte Marshalling: (pos:bytesT) = " + pos + ":" + bytesT);
			
			priorDocTerms.clear();
			keywordHash = Storable.getInt(pos, existingB);
			pos = pos + 4;

			/**
			 * Compute number of terms presence.
			 */
			termsT = existingB[pos++];
			if ( -1 == termsT ) {
				termsT =  Storable.getInt(pos, existingB);
				pos = pos + 4;
			} 
			if ( L.l.isDebugEnabled() ) L.l.debug("termsT:" + termsT + ":" + pos );
			
			/**
			 * Compute Each Term.
			 */
			shift = TermList.TERM_SIZE_NOVECTOR;
			if ( TermList.termVectorStorageEnabled ) shift = TermList.TERM_SIZE_VECTOR;
			for ( int i=0; i<termsT; i++) {
				if ( L.l.isDebugEnabled() ) L.l.debug("pos:" + pos );
				
				readPos = pos + ((shift - 2) * termsT )+ (i * 2);
				docPos = (short) ((existingB[readPos] << 8 ) + 
					( existingB[++readPos] & 0xff ));
				
				if ( freshDocs.contains(docPos)) continue;
				
				docTyep = existingB[pos+i];
				termTyep = existingB[pos + termsT + i];
				termWeight = existingB[pos + (2 * termsT) + i];
				
				if ( TermList.termVectorStorageEnabled ) {
					termFreq = existingB[pos + (3 * termsT) + i];
					readPos = pos + (4 * termsT) + i;
					termPos = (short) ( (existingB[readPos] << 8 ) + 
							( existingB[++readPos] & 0xff ) );
				}
				Term priorTerm = new Term(docPos,docTyep,termTyep,termWeight,termPos,termFreq);
				priorDocTerms.add(priorTerm);
			}

			if ( TermList.termVectorStorageEnabled ) pos = pos + (8 * termsT);
			else pos = pos + (5 * termsT);
			mergePrior(lstKeywords, priorDocTerms, keywordHash);
		}
	}

	/**
	 * Merge prior documents
	 * @param lstKeywords
	 * @param priorDocTerms
	 * @param keywordHash
	 */
	private static void mergePrior(Map<Integer, List<Term>> lstKeywords,
		List<Term> priorDocTerms, int keywordHash) {
		
		if ( priorDocTerms.size() > 0 ) {
			List<Term> terms = null;
			if ( lstKeywords.containsKey(keywordHash) ) { //This Keyword exists
				terms = lstKeywords.get(keywordHash);
				terms.addAll(priorDocTerms);
			} else {
				lstKeywords.put(keywordHash, priorDocTerms);
			}
		}
	}
	
	/**
	 * Get the fresh documentrs (The document position are absent)
	 * @param lstKeywords
	 * @return
	 */
	private static Set<Short> getFreshDocs(Map<Integer, List<Term>> lstKeywords) {
		Set<Short> freshDocs = new HashSet<Short>();
		short docPos;
		for (int hash : lstKeywords.keySet()) {
			List<Term> terms = lstKeywords.get(hash);
			for (Term term : terms) {
				docPos = term.getDocumentPosition();
				if ( freshDocs.contains(docPos)) continue;
				freshDocs.add(docPos);
			}
		}
		if ( L.l.isDebugEnabled() ) 
			L.l.debug("Fresh Documents:" + freshDocs.toString());
		return freshDocs;
	}

	public String toString() {
		
		StringBuilder sb = new StringBuilder();
		sb.append("Hash: [").append(hash);

		sb.append("]\nDocument Type: [");
		if ( null != dtc) for (byte c : dtc) sb.append(c).append(',');

		sb.append("]\nTerm Type: [");
		if ( null != ttc) for (byte c : ttc) sb.append(c).append(',');

		sb.append("]\nTerm Weight: [");
		if ( null != tw) for (byte w : tw) sb.append(w).append(',');
		
		sb.append("]\nTerm Frequency: [");
		if ( null != termFreq) for (byte tf : termFreq) sb.append(tf).append(',');
		
		sb.append("]\nTerm Position: [");
		if ( null != termPos) for (short tp : termPos) sb.append(tp).append(',');

		sb.append("]\nDocument Position = [");
		if ( null != docPos) for (short dp : docPos) sb.append(dp).append(',');
		sb.append(']');
		
		return sb.toString();
	}	
	
}
