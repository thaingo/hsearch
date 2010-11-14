package com.bizosys.hsearch.query;

public class Location {

	public Integer locId;
	public String country;
	public String region;
	public String city;
	public String postalCode;
	public Float latitude;
	public Float longitude;
	public String metroCode;
	public String areaCode;

	/** Default constructor */
	public Location() {
	}


	/** Constructor with primary keys (Insert with primary key)*/
	public Location(Integer locId,String country,String region,String city,
		String postalCode,Float latitude,Float longitude,
		String metroCode,String areaCode) {

		this.locId = locId;
		this.country = country;
		this.region = region;
		this.city = city;
		this.postalCode = postalCode;
		this.latitude = latitude;
		this.longitude = longitude;
		this.metroCode = metroCode;
		this.areaCode = areaCode;

	}


	/** Params for (Insert with autoincrement)*/
	public Object[] getNewPrint() {
		return new Object[] {
			locId, country, region, city, postalCode, latitude, 
			longitude, metroCode, areaCode
		};
	}


	/** Params for (Insert with primary key)*/
	public Object[] getNewPrintWithPK() {
		return new Object[] {
			locId, country, region, city, postalCode, latitude, 
			longitude, metroCode, areaCode
		};
	}


	/** Params for (Update)*/
	public Object[] getExistingPrint() {
		return new Object[] {
			locId, country, region, city, postalCode, latitude, 
			longitude, metroCode, areaCode
		};
	}

}
