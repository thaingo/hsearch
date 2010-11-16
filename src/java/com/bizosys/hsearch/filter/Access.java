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
	 
	 
	 StorableList list = new StorableList();
	 
	 public Access() {
	 }
	 
	 public Access(byte[] bytes) {
		this.list = new StorableList(bytes);
	}
	 
	 public void addAnonymous() {
		 list.add((UIDC + ANY).getBytes() );
	}
	
	public void addAcl(String acl) {
		 list.add(acl.getBytes());
	}
	
	public void addUid(String uid) {
		list.add((UIDC + uid).getBytes() );
	}
	
	public void addRole(String role) {
		list.add((ROLEC + role).getBytes() );
	}

	public void addTeam(String team) {
		list.add((TEAMC + team).getBytes() );
	}

	public void addOrgUnit(String ou) {
		list.add((OUC + ou).getBytes() );
	}

	public void addOrgUnitAndUid(String ou, String uid) {
		list.add((OU_UIDC + ou + "." + uid).getBytes() );
	}
	
	public void addOrgUnitAndRole(String ou, String role) {
		list.add((OU_ROLEC + ou + "." + role).getBytes() );
	}
	
	public StorableList getAccessList() {
		return list;
	}
	
	public void clear() {
		if ( null != this.list) this.list.clear();
	}
}