package com.bizosys.hsearch.filter;

/**
 * TODO:: Allow the regular expression here.
 * @author karan
 *
 */
public class Access {

	public static final char UIDC = '0';
	public static final char TEAMC = '1';
	public static final byte ROLEC = '2';
	public static final char OUC = '3';
	public static final char OU_UIDC = '4';
	public static final char OU_ROLEC = '5';

	public static final String ANY = "*";
	public static final byte[] ANY_BYTES = "0*".getBytes();
	 
	 
	 AccessStorable storable = new AccessStorable();
	 
	 public Access() {
	 }
	 
	 public Access(byte[] bytes) {
		this.storable = new AccessStorable(bytes);
	 }
	 
	 public void addAnonymous() {
		 storable.add((UIDC + ANY).getBytes() );
	}
	
	public void addAcl(String acl) {
		 storable.add(acl.getBytes());
	}
	
	public void addUid(String uid) {
		storable.add((UIDC + uid).getBytes() );
	}
	
	public void addRole(String role) {
		storable.add((ROLEC + role).getBytes() );
	}

	public void addTeam(String team) {
		storable.add((TEAMC + team).getBytes() );
	}

	public void addOrgUnit(String ou) {
		storable.add((OUC + ou).getBytes() );
	}

	public void addOrgUnitAndUid(String ou, String uid) {
		storable.add((OU_UIDC + ou + "." + uid).getBytes() );
	}
	
	public void addOrgUnitAndRole(String ou, String role) {
		storable.add((OU_ROLEC + ou + "." + role).getBytes() );
	}
	
	public AccessStorable toStorable() {
		return storable;
	}
	
	public void clear() {
		if ( null != this.storable) this.storable.clear();
	}
}