package com.bizosys.hsearch.query;

import java.util.HashMap;
import java.util.Map;

public class ReserveQueryWord {
	private static ReserveQueryWord instance = null; 
	public static ReserveQueryWord getInstance() {
		if ( null != instance) return instance;
		instance = new ReserveQueryWord();
		return instance;
	}
	
	private Map<String, Integer> reserveWord = 
		new HashMap<String, Integer>();
	
	public static final int NO_RESERVE_WORD = -1;
	
	public static final int RESERVE_id = 0;
	public static final int RESERVE_docType = 3;
	public static final int RESERVE_state = 4;
	public static final int RESERVE_orgunit = 5;
	public static final int RESERVE_bornBefore = 6;
	public static final int RESERVE_bornAfter = 7;
	public static final int RESERVE_touchAfter = 8;
	public static final int RESERVE_touchBefore = 9;
	public static final int RESERVE_areaInKmRadius = 10;
	public static final int RESERVE_matchIp = 11;
	public static final int RESERVE_latlng = 12;
	public static final int RESERVE_scroll = 13;
	public static final int RESERVE_boostTermWeight = 14;
	public static final int RESERVE_boostDocumentWeight = 15;
	public static final int RESERVE_boostIpProximity = 16;
	public static final int RESERVE_boostOwner = 17;
	public static final int RESERVE_boostFreshness = 18;
	public static final int RESERVE_boostPrecious = 19;
	public static final int RESERVE_boostChoices = 20;
	public static final int RESERVE_boostMultiphrase = 21;
	public static final int RESERVE_facetFetchLimit = 22;
	public static final int RESERVE_metaFetchLimit = 23;
	public static final int RESERVE_documentFetchLimit = 24;	
	public static final int RESERVE_teaserSectionLength = 25;
	
	public static final int RESERVE_cluster = 100;	
	public static final int RESERVE_sortOnMeta = 101;	
	public static final int RESERVE_sortOnField = 102;	
	public static final int RESERVE_metaFields = 103;
	public static final int RESERVE_touchstones = 104;	

	private ReserveQueryWord() {
		reserveWord.put("olid", RESERVE_id);
		reserveWord.put("typ", RESERVE_docType);
		reserveWord.put("ste", RESERVE_state);
		reserveWord.put("ou", RESERVE_orgunit);
		reserveWord.put("bornb", RESERVE_bornBefore);
		reserveWord.put("borna", RESERVE_bornAfter);
		reserveWord.put("toucha", RESERVE_touchAfter);
		reserveWord.put("touchb", RESERVE_touchBefore);
		reserveWord.put("aikr", RESERVE_areaInKmRadius);
		reserveWord.put("matchip", RESERVE_matchIp);
		reserveWord.put("latlng", RESERVE_latlng);
		reserveWord.put("scroll", RESERVE_scroll);
		reserveWord.put("termw", RESERVE_boostTermWeight);
		reserveWord.put("docbst", RESERVE_boostDocumentWeight);
		reserveWord.put("ipbst", RESERVE_boostIpProximity);
		reserveWord.put("ownerbst", RESERVE_boostOwner);
		reserveWord.put("freshbst", RESERVE_boostFreshness);
		reserveWord.put("preciousbst", RESERVE_boostPrecious);
		reserveWord.put("choicebst", RESERVE_boostChoices);
		reserveWord.put("mpbst", RESERVE_boostMultiphrase);
		reserveWord.put("ffl", RESERVE_facetFetchLimit);
		reserveWord.put("mfl", RESERVE_metaFetchLimit);
		reserveWord.put("dfl", RESERVE_documentFetchLimit);
		reserveWord.put("tsl", RESERVE_teaserSectionLength);
		
		reserveWord.put("cluster", RESERVE_cluster);
		reserveWord.put("som", RESERVE_sortOnMeta);
		reserveWord.put("sof", RESERVE_sortOnField);
		reserveWord.put("mf", RESERVE_metaFields);
		reserveWord.put("ts", RESERVE_touchstones);
	}
	
	/**
	 * Returns -1 is the word is not a reserved one.
	 * else returns the corresponding reserve word.
	 * @param word
	 * @return
	 */
	public int mapReserveWord(String word) {
		if ( reserveWord.containsKey(word)) {
			return reserveWord.get(word);
		} else {
			return NO_RESERVE_WORD;
		}
	}
}
