package com.bizosys.hsearch.common;

import com.bizosys.hsearch.filter.Access;

/**
 * Carries all access information. It is serializable across wire (REST API).
 * @author karan
 *
 */
public class AccessDefn {
	/**
	 * The User Ids
	 */
	public String[] uids = null;
	
	/**
	 * The Teams
	 */
	public String[] teams = null;
	
	/**
	 * The Roles
	 */
	public String[] roles = null;

	/**
	 * The Organization Units / Tenants
	 */
	public String[] ous = null;
	
	/**
	 * Specific Users from Organization Units 
	 */
	public String[][] ouAndUids = null;

	/**
	 * Specific Roles from Organization Units
	 */
	public String[][] ouAndRoles = null;
	
	/**
	 * Creates an Access Object from the provided access definition 
	 * @return	Access Object
	 */
	public Access getAccess() {
		
		if ( null == uids && null == teams && null == roles &&
			null == ous && null == ouAndUids && null == ouAndRoles) return null;
		
		Access access = new Access();
		if (null != uids) for (String uid : uids) access.addUid(uid);
		if (null != teams) for (String team : teams) access.addTeam(team);
		if (null != roles) for (String role : roles) access.addRole(role);
		if (null != ous) for (String ou : ous) access.addOrgUnit(ou);
		if (null != ouAndUids) for (String[] ouU : ouAndUids) access.addOrgUnitAndUid(ouU[0], ouU[1]);
		if (null != ouAndRoles) for (String[] ouR : ouAndRoles) access.addOrgUnitAndRole(ouR[0], ouR[1]);
		
		return access;
	}

}
