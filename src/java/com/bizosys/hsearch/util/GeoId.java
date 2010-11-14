package com.bizosys.hsearch.util;

import org.apache.oneline.util.StringUtils;


public class GeoId {
	private float northing; //In Kms
	private float easting;  //In Kms
	public int longZone;
	public char latZone;
	
	public GeoId() {
	}
	
	public int getNorthingInMeters() {
		return (int) northing * 1000;
	}

	public float getNorthingInKms() {
		return northing;
	}

	public void setNorthingInKms(float northing) {
		this.northing = northing;
	}
	
	public void setNorthingInKms(int northing) {
		this.northing = northing / 1000;
	}	
	
	//////////////
	public int getEasteringInMeters() {
		return (int) easting * 1000;
	}

	public float getEasteringInKms() {
		return easting;
	}
	
	public void setEasteringInKms(float easting) {
		this.easting = easting;
	}
	
	public void setEasteringInKms(int easting) {
		this.easting = easting / 1000;
	}
	
	/**
     * This is in Kms
     * @param house
     * @param northing (Kms)
     * @param easting (Kms)
     */
	public GeoId (String house, float northing, float easting) {
    	this.northing = northing;
    	this.easting = easting;
    	this.setHouse(house);
    }

    /**
     * 
     * @param house
     * @param northing (Mts)
     * @param easting (Mts)
     */
	public GeoId (String house, int northing, int easting) {
    	this.northing = northing / 1000;
    	this.easting = easting / 1000;
    	this.setHouse(house);
    }
    
    /**
     * 
     * @param latZone
     * @param longZone
     * @param northing (Mts)
     * @param easting (Mts)
     */
	public GeoId (char latZone, int longZone, int northing, int easting) {
    	this.northing = northing / 1000;
    	this.easting = easting / 1000;
    	this.latZone = latZone;
    	this.longZone = longZone;
    }

    public void setHouse(String house) {
    	String[] x = StringUtils.getStrings(house, ' ');
    	this.latZone = x[0].charAt(0);
    	this.longZone = Integer.parseInt(x[1]);
    }
    
    public String getHouse() {
    	return this.latZone + " " + this.longZone;
    }
    
    public void clone(GeoId dolly) {
    	this.northing = dolly.northing;
    	this.easting = dolly.easting;
    	this.longZone = dolly.longZone;
    	this.latZone = dolly.latZone;
    }
    
    /**
     * Converts for a given LatLng to Mercator Northing and Easting
     * @param lat
     * @param lng
     * @return
     */
    public static GeoId convertLatLng (double lat, double lng) {
    	CoordinateConversion x = new CoordinateConversion();
    	return x.latLon2UTM(lat, lng);
    }
    
    /**
     * Converts for a given LatLng to Mercator Northing and Easting
     * @param lat
     * @param lng
     * @return
     */
    public static GeoId convertLatLng (float lat, float lng) {
    	CoordinateConversion x = new CoordinateConversion();
    	return x.latLon2UTM(lat, lng);
    }
    
    
    public boolean isInProximity(GeoId geoId, float radiusInKm) {
    	return isInProximity(geoId.easting, geoId.northing, radiusInKm);
    }
    
    /**
     * 
     * @param easteringInKms (In Kms)
     * @param northingInKms (In Kms)
     * @param radiusInKm 
     * @return
     */
    public boolean isInProximity(float easteringInKms, float northingInKms, float radiusInKm) {
    	
    	float sideA, sideB; 
    	
    	if (easteringInKms > easting) {
    		sideA = easteringInKms - easting;
    	} else {
    		sideA = easting - easteringInKms;
    	}
    	
    	if (northingInKms > northing) {
    		sideB = northingInKms - northing;
    	} else {
    		sideB = northing - northingInKms;
    	}
    	
    	if ( sideA > sideB) {
    		if ( (sideB + radiusInKm - sideA) < 0 ) return false;
    	} else {
    		if ( (sideA + radiusInKm - sideB) < 0 ) return false;
    	}
    	double distance = Math.sqrt( sideA * sideA + sideB * sideB );
    	if ( distance > radiusInKm) return false;
    	return true;
    }
    
    /**
     * 
     * @return
     */
    public float[] getLatLng() {
    	CoordinateConversion x = new CoordinateConversion();
    	return x.utm2LatLon(this);
    }

    public String toString() {
    	StringBuilder sb = new StringBuilder(40);
    	sb.append(this.longZone ).append(' ')
    		.append(this.latZone ).append(' ')
    		.append(this.easting ).append(' ')
    		.append(this.northing );
    	return  sb.toString();   
    }
    
}
