package com.bizosys.hsearch.common;

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
		 list.add(new Storable(UIDC + ANY) );
	}
	
	public void addAcl(String acl) {
		 list.add(new Storable(acl));
	}
	
	public void addUid(String uid) {
		list.add(new Storable(UIDC + uid) );
	}
	
	public void addRole(String role) {
		list.add(new Storable(ROLEC + role) );
	}

	public void addTeam(String team) {
		list.add(new Storable(TEAMC + team) );
	}

	public void addOrgUnit(String ou) {
		list.add(new Storable(OUC + ou) );
	}

	public void addOrgUnitAndUid(String ou, String uid) {
		list.add(new Storable(OU_UIDC + ou + "." + uid) );
	}
	
	public void addOrgUnitAndRole(String ou, String role) {
		list.add(new Storable(OU_ROLEC + ou + "." + role) );
	}
	
	public StorableList getAccessList() {
		return list;
	}
	
	public void clear() {
		if ( null != this.list) this.list.clear();
	}
}