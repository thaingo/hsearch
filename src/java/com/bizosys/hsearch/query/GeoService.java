package com.bizosys.hsearch.query;

import java.util.List;

import com.oneline.dao.ReadObject;

/**
/**
 * Resolves LatLng from a given IP address.
 *  
	CREATE TABLE  `geo`.`blocks` (
	  `startIp` decimal(10,0) NOT NULL,
	  `endIp` decimal(10,0) NOT NULL,
	  `locId` decimal(10,0) NOT NULL
	);
	
	com.oneline.is.dao.util.FileWriterUtil removeQuotes d:/downloads/GeoLiteCity-Blocks.csv  d:/downloads/GeoLiteCity-Blocks.csv.noquote	

 	LOAD DATA INFILE "D:/downloads/GeoLiteCity-Blocks.csv.noquote" INTO TABLE blocks
	FIELDS TERMINATED BY "," LINES TERMINATED BY '\r\n';
	
	---------
	CREATE TABLE  city (
	  locId decimal(10,0) NOT NULL,
	  country char(2) DEFAULT NULL,
	  region char(2) DEFAULT NULL,
	  city varchar(100) DEFAULT NULL,
	  postalCode char(10) DEFAULT NULL,
	  latitude float NOT NULL,
	  longitude float NOT NULL,
	  metroCode varchar(10) DEFAULT NULL,
	  areaCode varchar(10) DEFAULT NULL
	);
	
	
	select locid from geo.blocks where 2079165448 >= startIp AND  2079165448 <= endIp
	select latitude, longitude from city where locid = 20067
	
	 * @author karan
 *
 */
public class GeoService {
	
	public static final String selectStmt = 
		"select latitude, longitude from city,blocks" + 
		" where city.locid = blocks.locid" + 
		" AND ? >= startIp  AND ? <= endIp";

	public static final String source = "geo"; 
	
	public Location getLocation(int ipHashed) throws Exception {
		
		List listObject = new ReadObject().execute(
			selectStmt, new Object[]{ipHashed, ipHashed}, Location.class);
		
		if ( null != listObject && listObject.size() == 1) {
			return (Location) listObject.get(0);
		}
		return null;
	}	
}
