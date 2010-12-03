package com.bizosys.hsearch.filter;

public class AccessDefn {
	public String[] uids = null;
	public String[] teams = null;
	public String[] roles = null;
	public String[] ous = null;
	public String[][] ouAndUids = null;
	public String[][] ouAndRoles = null;
	
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
