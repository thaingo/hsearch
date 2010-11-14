package com.bizosys.hsearch.query;

import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import org.apache.oneline.ApplicationFault;
import org.apache.oneline.util.StringUtils;

import com.bizosys.hsearch.common.ByteField;
import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.security.WhoAmI;
import com.bizosys.hsearch.util.GeoId;
import com.bizosys.hsearch.util.IpUtil;

public class QueryContext {
	
	/**
	 * It contains the document and their weights
	 * bucket_docpos	weight
	 */
	public Map<String, Float> docweight = new Hashtable<String, Float>(250);
	
	public String id = null;

	public QueryContext(String queryString) throws ApplicationFault {
		this.queryString = queryString;
	}


	/* ************************************************
	 * Who and When this querying happend *****
	 * ***********************************************/
	
	/**
	 * The browser agent type. //User-Agent 
	 */
	public String clientType = null;
	
	/**
	 * The ip address of the searcher
	 */
	public String ipAddress = null;
	
	/**
	 * The user who is accessing it.  
	 */
	public WhoAmI user = null;

	public long currentTime = new Date().getTime();
	

	/* ************************************************
	 * The filteration criteria applied to search *****
	 * ***********************************************/

	/**
	 * Query terms Must and optional
	 */
	public String queryString = null;
	
	/**
	 * The document type which needs to be scanned
	 * Table Name / File Extension / Dna Name   
	 */
	public String docType = null;

	/**
	 * The document type which needs to be scanned
	 * Table Name / File Extension / Dna Name   
	 */
	public Byte docTypeCode = null;
	
	
	/**
	 * The state of the docucment (Applied, Processed)
	 */
	public Storable state = null;

	/**
	 * Just the department (HR, PRODUCTION)
	 */
	public Storable orgUnit = null;

	/**
	 * Born Before Date
	 */
	public Long bornBefore = null;

	/**
	 * Born After Date
	 */
	public Long bornAfter = null;

	/**
	 * Touched After Date
	 */
	public Long touchAfter = null;

	/**
	 * Touched Before Date
	 */
	public Long touchBefore = null;
	
	/**
	 * Area proximity where things will be looked into.
	 */
	public int areaInKmRadius = -1;
	
	/**
	 * Look documents created from this IP only
	 */
	public String matchIp = null;
	
	/**
	 * The latitude, longitude
	 */
	private GeoId geoId = null;
	public void setGeoId(GeoId geoId){
		this.geoId = geoId;
	}
	
	public GeoId getGeoId() throws ApplicationFault {
		if ( null != geoId) return geoId;
		
		if ( null == this.ipAddress 
			|| "0.0.0.0".equals(this.ipAddress)
			|| "127.0.0.1".equals(this.ipAddress)
			|| "localhost".equals(this.ipAddress)) {
			return null;
		}
			
		int ip = IpUtil.computeHouse(this.ipAddress);
		Location loc = null;
		try {
			loc = new GeoService().getLocation(ip);
		} catch (Exception ex) {
			L.l.fatal("outflow.QueryContext GeoService Invocation Failure", ex);
			throw new ApplicationFault(ex);
		}
			
		if ( null == loc) return null;
		geoId = GeoId.convertLatLng(loc.latitude, loc.longitude);
		return geoId; 
	}	

	/* ************************************************
	 * How to Rank *****
	 * ***********************************************/
	public int boostMultiPhrase = 1; //term as well as doc
	public int boostTermWeight = 1; //term as well as doc
	public int boostDocumentWeight = 1; //term as well as doc
	public int boostIpProximity = 1;
	public int boostOwner = 1;
	public int boostFreshness = 1;
	public int boostPrecious = 1;
	public int boostChoices = 1;
	
	/* ************************************************
	 * How much to retrieve *****
	 * ***********************************************/
	public int teaserSectionLen = -1; //Complete Document
	public int facetFetchLimit = 1000;
	public int metaFetchLimit = 1000;
	public int documentFetchLimit = 25;
	
	public String[] metaFields = null;
	
	public Map<String, String> sortOnMeta = null; //orgUnit:asc
	public Map<String, String> sortOnFld = null; //empName:asc
	public static String SORT_ASC = "asc";
	public static String SORT_DESC = "desc";
	
	public static String CLUSTER_NLP = "nlp";
	public static String CLUSTER_META = "meta";
	public static String CLUSTER_STRUCTURE = "structure";
	public Map<String, String> cluster = null; //orgUnit=META
	
	/**
	 * A analysis report on the QueryContext
	 */
	public Map<byte[][], byte[]> equalsMatching = null;
	public Map<byte[][], Long> lessThanMatching = null;
	public Map<byte[][], Long> greaterThanMatching = null;
	public Map<ByteField, Integer> metaMatching = null;
	public boolean isTouchStone = false;
	public int totalTerms = 0;
	
	/**
	 * Hints
	 */
	public boolean isOrgUnit = false;
	public boolean isDocType = false;
	
	/**
	 * Matching phrases.. They are needed to compute the teaser.
	 */
	public String allMatchPhrase = null;
	public String twoMatchPhrase = null;
	public String firstMatchPhrase = null;
	public String getAllMatchPhrase() {
		return allMatchPhrase;
	}
	
	/**
	 * This populates the query Context element from the 
	 * Lucene Style Query String
	 * @param reserveWord
	 * @param value
	 */
	public void populate (int reserveWord, String value) throws ApplicationFault {
		if ( StringUtils.isEmpty(value)) return;
		
		value = value.replace('_', ' ').trim();
		if ( L.l.isInfoEnabled())
			L.l.info(value + " .. " + reserveWord);
		
		switch (reserveWord) {
		
		case ReserveQueryWord.RESERVE_id:
			this.id = value;
			break;

		case ReserveQueryWord.RESERVE_docType:
			this.docType = value;
			break;
		case ReserveQueryWord.RESERVE_state:
			this.state = new Storable(value);
			break;
		case ReserveQueryWord.RESERVE_orgunit:
			this.orgUnit = new Storable(value);
			break;
		case ReserveQueryWord.RESERVE_bornBefore:
			this.bornBefore = Long.parseLong(value);
			break;
		case ReserveQueryWord.RESERVE_bornAfter:
			this.bornAfter = Long.parseLong(value);
			break;
		case ReserveQueryWord.RESERVE_touchAfter:
			this.touchAfter = Long.parseLong(value);
			break;
		case ReserveQueryWord.RESERVE_touchBefore:
			this.touchBefore = Long.parseLong(value);
			break;
		case ReserveQueryWord.RESERVE_areaInKmRadius:
			this.areaInKmRadius = Integer.parseInt(value);
			break;
		case ReserveQueryWord.RESERVE_matchIp:
			this.matchIp = value;
			break;
		case ReserveQueryWord.RESERVE_latlng:
			String[] latlng = StringUtils.getStrings(value, ',');
			if ( null == geoId) this.geoId = GeoId.convertLatLng(
				Float.parseFloat(latlng[0]), Float.parseFloat(latlng[1]));
			break;
		case ReserveQueryWord.RESERVE_boostMultiphrase:
			this.boostMultiPhrase = Integer.parseInt(value);
			break;
		case ReserveQueryWord.RESERVE_boostTermWeight:
			this.boostTermWeight = Integer.parseInt(value);
			break;
		case ReserveQueryWord.RESERVE_boostDocumentWeight:
			this.boostDocumentWeight = Integer.parseInt(value);
			break;
		case ReserveQueryWord.RESERVE_boostIpProximity:
			this.boostIpProximity = Integer.parseInt(value);
			break;
		case ReserveQueryWord.RESERVE_boostOwner:
			this.boostOwner = Integer.parseInt(value);
			break;
		case ReserveQueryWord.RESERVE_boostFreshness:
			this.boostFreshness = Integer.parseInt(value);
			break;
		case ReserveQueryWord.RESERVE_boostPrecious:
			this.boostPrecious = Integer.parseInt(value);
			break;
		case ReserveQueryWord.RESERVE_boostChoices:
			this.boostChoices = Integer.parseInt(value);
			break;
		case ReserveQueryWord.RESERVE_facetFetchLimit:
			this.facetFetchLimit = Integer.parseInt(value);
			break;
		case ReserveQueryWord.RESERVE_metaFetchLimit:
			this.metaFetchLimit = Integer.parseInt(value);
			break;
		case ReserveQueryWord.RESERVE_documentFetchLimit:
			this.documentFetchLimit = Integer.parseInt(value);
			break;
		case ReserveQueryWord.RESERVE_teaserSectionLength:
			this.teaserSectionLen = Integer.parseInt(value);
			break;
		case ReserveQueryWord.RESERVE_metaFields:
			this.metaFields = StringUtils.getStrings(value, ",");
			break;

		case ReserveQueryWord.RESERVE_cluster:
			if ( null == cluster) cluster = new HashMap<String, String>();
			String[] values = StringUtils.getStrings(value, '=');
			validateClusteringStyle(values[1]);
			if ( values.length == 2) { //ID=META, BODY=NLP
				this.cluster.put(values[0],values[1]);
			} else {
				throw new ApplicationFault("Parsing Failure : Invalid cluster. Ex id=meta");
			}
			
			break;

		case ReserveQueryWord.RESERVE_sortOnMeta:
			if ( null == sortOnMeta) sortOnMeta = new HashMap<String, String>();
			String[] sortValues = StringUtils.getStrings(value, '=');

			switch (sortValues.length) {
				case 1:
					this.sortOnMeta.put(sortValues[0], SORT_ASC);
					break;
				case 2:
					validateSortingStyle(sortValues[1]);
					this.sortOnMeta.put(sortValues[0],sortValues[1]);
					break;
				default:
					throw new ApplicationFault("Parsing Failure : Invalid sorting. Ex id=asc");
			}
			break;
			
		case ReserveQueryWord.RESERVE_touchstones:
			this.isTouchStone = true;
			break;
		
			
		case ReserveQueryWord.RESERVE_sortOnField:
			if ( null == sortOnFld) sortOnFld = new HashMap<String, String>();
			String[] sortFldValues = StringUtils.getStrings(value, '=');
			switch (sortFldValues.length) {
			case 1:
				this.sortOnFld.put(sortFldValues[0], SORT_ASC);
				break;
			case 2:
				validateSortingStyle(sortFldValues[1]);
				this.sortOnFld.put(sortFldValues[0],sortFldValues[1]);
				break;

			default:
				throw new ApplicationFault("Parsing Failure : Invalid sorting. Ex empname=asc");
			}
			break;
		}
	}
	
	/**
	 * Needs to be either asc or desc
	 * @param sortType
	 * @throws ApplicationFault
	 */
	private void validateSortingStyle(String sortType) throws ApplicationFault {
		
		boolean isProperSorting = 
			SORT_ASC.equals(sortType) || SORT_DESC.equals(sortType)	;
		if ( !isProperSorting) {
			throw new ApplicationFault("Parsing Failure : Invalid sorting option. Ex " 
				+ SORT_ASC + " or " + SORT_DESC );
		}
	}
	
	/**
	 * Needs to be either asc or desc
	 * @param sortType
	 * @throws ApplicationFault
	 */
	private void validateClusteringStyle(String clusterType) throws ApplicationFault {
		
		boolean isProperClustering = 
			CLUSTER_NLP.equals(clusterType) || 
			CLUSTER_META.equals(clusterType) ||
			CLUSTER_STRUCTURE.equals(clusterType);
		if ( !isProperClustering) {
			throw new ApplicationFault("Parsing Failure : Invalid clustering option. Ex. " +  
				CLUSTER_NLP + " or " + CLUSTER_META + " or " + CLUSTER_STRUCTURE );
		}
	}	
	@Override
	public String toString() {
		
		StringBuilder sb = new StringBuilder();
		if ( null != id) sb.append("id =").append(id).append('\n');
		if ( null != clientType) sb.append("clientType =").append(clientType).append('\n');
		if ( null != ipAddress ) sb.append("ipAddress =").append(ipAddress ).append('\n');
		if ( null != user ) sb.append("user =").append(user.toString() ).append('\n');
		if ( null != queryString  ) sb.append("queryString =").append(queryString).append('\n');
		if ( null != docType   ) sb.append("docType  =").append(docType).append('\n');
		if ( null != state    ) sb.append("state   =").append(state.getValue()  ).append('\n');
		if ( null != orgUnit   ) sb.append("orgUnit  =").append(orgUnit.getValue() ).append('\n');
		if ( null != bornBefore   ) sb.append("bornBefore  =").append(new Date(bornBefore).toString() ).append('\n');
		if ( null != bornAfter    ) sb.append("bornAfter   =").append(new Date(bornAfter ).toString() ).append('\n');
		if ( null != touchAfter    ) sb.append("touchAfter   =").append(new Date(touchAfter ).toString() ).append('\n');
		if ( null != touchBefore    ) sb.append("touchBefore   =").append(new Date(touchBefore ).toString() ).append('\n');
		if ( -1 != areaInKmRadius  ) sb.append("areaInKmRadius    =").append(areaInKmRadius).append('\n');
		if ( null != matchIp    ) sb.append("matchIp =").append(matchIp  ).append('\n');
		if ( null != geoId ) sb.append("geoId  =").append(geoId.toString()   ).append('\n');
		if ( 1 != boostDocumentWeight  ) sb.append("docBoost  =").append(boostDocumentWeight ).append('\n');
		if ( 1 != boostIpProximity   ) sb.append("networkBoost   =").append(boostIpProximity  ).append('\n');
		if ( 1 != boostOwner   ) sb.append("authorBoost   =").append(boostOwner  ).append('\n');
		if ( 1 != boostFreshness   ) sb.append("freshnessBoost   =").append(boostFreshness  ).append('\n');
		if ( 1 != boostPrecious   ) sb.append("preciousBoost   =").append(boostPrecious  ).append('\n');
		if ( 1 != boostChoices    ) sb.append("choiceBoost    =").append(boostChoices   ).append('\n');
		sb.append("facetFetchLimit     =").append(facetFetchLimit    ).append('\n');
		sb.append("metaFetchLimit     =").append(metaFetchLimit    ).append('\n');
		sb.append("documentFetchLimit     =").append(documentFetchLimit    ).append('\n');
		if ( -1 != teaserSectionLen) sb.append("teaserSectionLen =").append(teaserSectionLen).append('\n');

		if ( null != cluster) sb.append("cluster =").append(cluster).append('\n');
		if ( null != sortOnMeta) sb.append("sortOnMeta =").append(sortOnMeta).append('\n');
		if ( null != sortOnFld) sb.append("sortOnFld =").append(sortOnFld).append('\n');
		if ( null != this.metaFields) {
			for (String metaFld: this.metaFields) {
				sb.append("Asked Meta =").append(metaFld).append('\n');
			}
		}
		
		return sb.toString();
	}
	
}
