package com.bizosys.hsearch.index;

import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.util.StringUtils;

/**
 * @author karan
 */
public class Term {
	
	public static Character TERMLOC_URL = 'U';
	public static Character TERMLOC_SUBJECT = 'S';
	public static Character TERMLOC_BODY = 'B';
	public static Character TERMLOC_META = 'M';
	public static Character TERMLOC_XML = 'X';
	public static Character TERMLOC_KEYWORD = 'K';
	
	public static String NO_TERM_TYPE = "";
	public static String TERMTYPE_ACRONUM = "ACR";
	public static String TERMTYPE_DATE = "DATE";
	public static String TERMTYPE_EMAIL = "MAIL";
	public static String TERMTYPE_ID = "ID";
	public static String TERMTYPE_URL = "URL";
	public static String TERMTYPE_NOUN = "NAME";
	public static String TERMTYPE_PHONE = "PHONE";
	public static String TERMTYPE_LINKTEXT = "LNKTXT";
	public static String TERMTYPE_MIME = "MM";
	
	/**
	 * This is the position from which we use position jump
	 * to calculate the position. 
	 */
	public static int POSITION_JUMP_FROM = 65000;
	
	/**
	 * This is the serial position of document in the bucket.
	 * A bucket will have capability to store till 65536 documents
	 */
	private short docPos = Short.MIN_VALUE;
	
	/**
	 * The document type (Variation 256)
	 * Now we can support total 256 types of document.
	 * We can map one ID for different document type. 
	 * This will later can be filtered reading the meta fields 
	 * (Low probability of clashing) 
	 */
	private byte docTypeCode = Byte.MIN_VALUE;
	
	/**
	 * The Term type (Variation 256)
	 * Now we can support total 256 types of term types.
	 * This is OK as we can map multiple types to same id
	 * avoiding duplication in docType level (Low probability of clashing) 
	 */
	private byte termTypeCode = Byte.MIN_VALUE ;
	
	/**
	 * Term Weight will be from 0-256
	 */
	private byte weight = Byte.MIN_VALUE;
	
	/**
	 * Position of term in the document
	 */
	private short termPos = Short.MIN_VALUE;
	
	/**
	 * The frequency of term in the document
	 */
	private byte termFreq = 1;

	
	/**
	 * Intermediate computation fields
	 */
	public String term;
	public String termType;
	public Character sightting;
	
	public Term() {
	}
	
	/**
	 * The stored term
	 * @param docPos
	 * @param docTypeCode
	 * @param termTypeCode
	 * @param weight
	 * @param termPos
	 * @param termFreq
	 * @throws ApplicationFault
	 */
	public Term(short docPos, byte docTypeCode, byte termTypeCode, 
		byte weight, short termPos, byte termFreq ) {
			
		this.docPos = docPos;
		this.docTypeCode = docTypeCode;
		this.termTypeCode = termTypeCode;
		this.weight = weight;
		this.termPos = termPos;
		this.termFreq = termFreq;
	}	
	
	/**
	 * These fields are
	 * @param term
	 * @param sightting
	 * @param typeInput
	 * @param termPos
	 * @throws ApplicationFault
	 */
	public Term(String term, Character sightting, 
		String termType, Integer termPos ) throws ApplicationFault {
		
		if ( StringUtils.isEmpty(term) ) return;
		
		this.term = term;
		this.termType = termType;
		if ( null != termType ) 
			this.termTypeCode = TermType.getInstance().getTypeCode(termType);
		
		this.sightting = sightting;
		this.termPos = 	new Integer(termPos ).shortValue();
	}
	
	public Term(String term, Character sightting, 
		byte termTypeCode, Integer termPos ) {
			
		if ( StringUtils.isEmpty(term) ) return;
		
		this.term = term;
		this.termTypeCode = termTypeCode;
		this.sightting = sightting;
		this.termPos = 	new Integer(termPos ).shortValue();
	}
	
	public Term(String term, Character sightting, 
		byte termTypeCode, Integer termPos, short docPos, byte termWeight ) {
		
		this(term,sightting,termTypeCode,termPos);
		this.setDocumentPosition(docPos);
		this.setTermWeight(termWeight);
			
	}	
	
	
	public void resetTerm(String term) {
		this.docPos = Short.MIN_VALUE;
		this.docTypeCode = Byte.MIN_VALUE;
		this.termTypeCode = Byte.MIN_VALUE ;
		this.termTypeCode = Byte.MIN_VALUE ;
		this.weight = Byte.MIN_VALUE;
		this.termPos = Short.MIN_VALUE;
	}
	
	/**
	 * This will be from -1 till 65530
	 * 32232 + Increment 1 for each 100000 (This is After 65000)  
	 * @param termPos
	 * @return
	 */
	public short setTermPos(int termPos) {
		
		if ( termPos < 65000 ) {
			short termPosCur = new Integer(termPos).shortValue();
			return (short)(Short.MIN_VALUE + termPosCur + 1);
		}
		
		int jump  =  (termPos - 65000) / 100000;
		return (short)( 32232 + jump );
	}
	
	public int getTermPos(short termPos) {
		
		if ( termPos <= 32232 ) {
			return ( (-1 * Short.MIN_VALUE) + termPos - 1);
		}
		
		int jump = (termPos - 32232) * 100000;
		return 65000 + jump ;
	}
	
	/**
	 * We are discounting the term count. Rather we are counting
	 * the sighting location for merging.
	 * @param term
	 */
	public boolean merge(Term term) {
		
		/**
		 * Not the term from same document
		 */
		if ( this.docPos != term.docPos) return false;
		
		/*
		 * Term repetition in the same document 
		 */
		if ( term.weight > this.weight) {
			this.weight = term.weight;
			this.sightting = term.sightting;
			if ( -1 != term.termPos ) this.termPos = term.termPos;
		}
		
		int totalFreq = this.termFreq + term.termFreq;
		if (totalFreq > Byte.MAX_VALUE) this.termFreq = Byte.MAX_VALUE;
		else this.termFreq = (byte) totalFreq;
		return true;
	}
	
	public short getDocumentPosition() {
		return this.docPos;		
	}
	
	public void setDocumentPosition(short pos) {
		System.out.println("Set Position :" + pos);
		this.docPos = pos;		
	}
	
	public byte getDocumentTypeCode() {
		return this.docTypeCode;		
	}
	
	public void setDocumentTypeCode(byte type) {
		this.docTypeCode = type;
	}

	public byte getTermTypeCode() {
		return this.termTypeCode;		
	}
	
	public void setTermTypeCode(byte type) {
		this.termTypeCode = type;
	}

	public short getTermPosition() {
		return this.termPos;		
	}
	
	public void setTermWeight(short termPos ) {
		this.termPos = termPos;
	}
	
	public byte getTermWeight() {
		return this.weight;		
	}
	
	public void setTermWeight(byte weight) {
		this.weight = weight;
	}
	
	public byte getTermFrequency() {
		return this.termFreq;		
	}
	
	public void setTermFrequency(byte termFreq) {
		this.termFreq = termFreq;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Term :" ).append(term);
		sb.append(" , Doc Pos :" ).append(docPos);
		sb.append(" , Doc Tyoe :" ).append(docTypeCode);
		sb.append(" , Term Pos :" ).append(termPos);
		sb.append(" , Term Type :" ).append(termType);
		sb.append(" , Term Freq :" ).append(termFreq);
		sb.append(" , Term Weight :" ).append(weight);
		return sb.toString();
	}	
	
	public static void main(String[] args) {
		Term t = new Term();
		System.out.println( "0 = " + t.getTermPos(t.setTermPos(0)) );
		System.out.println( "23 = " + t.getTermPos(t.setTermPos(23)) );
		System.out.println( "64998 = " + t.getTermPos(t.setTermPos(64998)) );
		System.out.println( "64999 = " + t.getTermPos(t.setTermPos(64999)) );
		System.out.println( "65000 = " + t.getTermPos(t.setTermPos(65000)) );
		System.out.println( "65001 = " + t.getTermPos(t.setTermPos(65001)) );
		System.out.println( "65002 = " + t.getTermPos(t.setTermPos(65002)) );
		
		System.out.println( "42234 = " + t.getTermPos(t.setTermPos(42234)) );
		System.out.println( "-1 = " + t.getTermPos(t.setTermPos(-1)) );
		System.out.println( "36435345 = " + t.getTermPos(t.setTermPos(36435345)) );
		System.out.println( "482324 = " + t.getTermPos(t.setTermPos(482324)) );
		System.out.println( "7823435 = " + t.getTermPos(t.setTermPos(7823435)) );
		System.out.println( "-2134324 = " + t.getTermPos(t.setTermPos(-2134324)) );
		
		Term term = new Term();
		term.termFreq = 122;
		
		Term term2 = new Term();
		term2.termFreq = 2;
		
		term.merge(term2);
		System.out.println("Term Frequency :" + term.termFreq);
	}
}
