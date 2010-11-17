package com.bizosys.hsearch.security;

import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestCase;


import com.bizosys.ferrari.TestFerrari;
import com.bizosys.hsearch.filter.Access;
import com.bizosys.hsearch.filter.AccessList;
import com.bizosys.hsearch.security.AccessControl;
import com.bizosys.hsearch.security.WhoAmI;

public class AccessControlTest extends TestCase {

	public static void main(String[] args) throws Exception {
        Test t = new AccessControlTest();
        TestFerrari.testAll(t);
	}
	
	public void testUser(String uid) throws Exception {
		//Set user
		WhoAmI firstUser = new WhoAmI();
		firstUser.uid = uid;
		
		//Serialize access
		Access access = new Access();
		access.addUid(uid);
		AccessList myAccess = access.getAccessList();
		byte[] persist = myAccess.toBytes();
		
		//Deserialize access
		Access setAccess = new Access(persist);
		AccessList accessBytes = setAccess.getAccessList(); 
		
		//Check access
		Assert.assertTrue(AccessControl.hasAccess(firstUser, accessBytes));
		
		WhoAmI secondUser = new WhoAmI();
		secondUser.uid = "XYZ";

		//Check access
		Assert.assertFalse(AccessControl.hasAccess(secondUser, accessBytes));
		
	}
	
	public void testRole(String role) throws Exception {
		//Set user
		WhoAmI firstUser = new WhoAmI();
		firstUser.roles = new String[]{role};
		
		//Serialize access
		Access access = new Access();
		access.addRole(role);
		AccessList myAccess = access.getAccessList();
		byte[] persist = myAccess.toBytes();
		
		//Deserialize access
		Access setAccess = new Access(persist);
		AccessList accessBytes = setAccess.getAccessList(); 
		
		//Check access
		Assert.assertTrue(AccessControl.hasAccess(firstUser, accessBytes));
		
		WhoAmI secondUser = new WhoAmI();
		secondUser.uid = "XYZ";

		//Check access
		Assert.assertFalse(AccessControl.hasAccess(secondUser, accessBytes));
		
		secondUser.roles = new String[]{"ROLE1","ROLE2"};
		Assert.assertFalse(AccessControl.hasAccess(secondUser, accessBytes));
		
		secondUser.roles = new String[]{"ROLE1","ROLE2", role};
		Assert.assertTrue(AccessControl.hasAccess(secondUser, accessBytes));
	}
	
	public void testRoles(String role1, String role2,String role3) throws Exception {
		//Set user
		WhoAmI firstUser = new WhoAmI();
		firstUser.roles = new String[] {role1};
		
		//Serialize access
		Access access = new Access();
		access.addRole(role1);
		access.addRole(role2);
		access.addRole(role3);
		AccessList myAccess = access.getAccessList();
		byte[] persist = myAccess.toBytes();
		
		//Deserialize access
		Access setAccess = new Access(persist);
		AccessList accessBytes = setAccess.getAccessList(); 
		
		//Check access
		Assert.assertTrue(AccessControl.hasAccess(firstUser, accessBytes));
		
		WhoAmI secondUser = new WhoAmI();
		secondUser.roles = new String[] {role2};
		Assert.assertTrue(AccessControl.hasAccess(secondUser, accessBytes));

		//Check access
		WhoAmI thirdUser = new WhoAmI();
		thirdUser.roles = new String[] {role3};
		Assert.assertTrue(AccessControl.hasAccess(thirdUser, accessBytes));

		WhoAmI unknownUser = new WhoAmI();
		unknownUser.roles = new String[] {"XX"};
		Assert.assertFalse(AccessControl.hasAccess(unknownUser, accessBytes));
	}		
	
	public void testOrgUnit(String unit) throws Exception {
		//Set user
		WhoAmI firstUser = new WhoAmI();
		firstUser.ou = unit;
		
		//Serialize access
		Access access = new Access();
		access.addOrgUnit(unit);
		AccessList myAccess = access.getAccessList();
		byte[] persist = myAccess.toBytes();
		
		//Deserialize access
		Access setAccess = new Access(persist);
		AccessList accessBytes = setAccess.getAccessList(); 
		
		//Check access
		Assert.assertTrue(AccessControl.hasAccess(firstUser, accessBytes));
		
		WhoAmI secondUser = new WhoAmI();
		secondUser.uid = "XYZ";
		secondUser.ou = "XYZ";

		//Check access
		Assert.assertFalse(AccessControl.hasAccess(secondUser, accessBytes));
		
		secondUser.ou = unit;
		Assert.assertTrue(AccessControl.hasAccess(secondUser, accessBytes));
		
	}
	
	public void testTeam(String team) throws Exception {
		//Set user
		WhoAmI firstUser = new WhoAmI();
		firstUser.teams = new String[] {team};
		
		//Serialize access
		Access access = new Access();
		access.addTeam(team);
		AccessList myAccess = access.getAccessList();
		byte[] persist = myAccess.toBytes();
		
		//Deserialize access
		Access setAccess = new Access(persist);
		AccessList accessBytes = setAccess.getAccessList(); 
		
		//Check access
		Assert.assertTrue(AccessControl.hasAccess(firstUser, accessBytes));
		
		WhoAmI secondUser = new WhoAmI();
		secondUser.teams = new String[] {"XYZ"};

		//Check access
		Assert.assertFalse(AccessControl.hasAccess(secondUser, accessBytes));
	}
	
	public void testTeams(String team1, String team2,String team3) throws Exception {
		//Set user
		WhoAmI firstUser = new WhoAmI();
		firstUser.teams = new String[] {team1};
		
		//Serialize access
		Access access = new Access();
		access.addTeam(team1);
		access.addTeam(team2);
		access.addTeam(team3);
		AccessList myAccess = access.getAccessList();
		byte[] persist = myAccess.toBytes();
		
		//Deserialize access
		Access setAccess = new Access(persist);
		AccessList accessBytes = setAccess.getAccessList(); 
		
		//Check access
		Assert.assertTrue(AccessControl.hasAccess(firstUser, accessBytes));
		
		WhoAmI secondUser = new WhoAmI();
		secondUser.teams = new String[] {team2};
		Assert.assertTrue(AccessControl.hasAccess(secondUser, accessBytes));

		//Check access
		WhoAmI thirdUser = new WhoAmI();
		thirdUser.teams = new String[] {team3};
		Assert.assertTrue(AccessControl.hasAccess(thirdUser, accessBytes));

		WhoAmI unknownUser = new WhoAmI();
		unknownUser.teams = new String[] {"XX"};
		Assert.assertFalse(AccessControl.hasAccess(unknownUser, accessBytes));
	}	

	public void testIBMArchitectOnly(String unknownUnit, String unknownRole) throws Exception {
		WhoAmI ibmArchitect = new WhoAmI("ibm","architect");

		Access access = new Access();
		access.addOrgUnitAndRole("ibm","architect");

		//Check access
		Assert.assertTrue(AccessControl.hasAccess(
			ibmArchitect, access.getAccessList()));
		
		Assert.assertFalse(AccessControl.hasAccess(
			new WhoAmI(unknownUnit,unknownRole), access.getAccessList()));
		
	}
	
	public void testIITAlumniAndBizosys(String unknownUnit, String unknownTeam) throws Exception {
		WhoAmI iitAlumniAndBizosys = new WhoAmI();
		iitAlumniAndBizosys.ou = "bizosys";
		iitAlumniAndBizosys.teams = new String[]{"iit"};

		Access access = new Access();
		access.addOrgUnit("bizosys");
		access.addTeam("iit");
		
		//Check access
		Assert.assertTrue(AccessControl.hasAccess(
			iitAlumniAndBizosys, access.getAccessList()));
		
		access.clear();
		access.addOrgUnit("bizosys");
		Assert.assertTrue(AccessControl.hasAccess(
			iitAlumniAndBizosys, access.getAccessList()));
		
		access.clear();
		access.addTeam("iit");
		Assert.assertTrue(AccessControl.hasAccess(
			iitAlumniAndBizosys, access.getAccessList()));

		access.clear();
		access.addOrgUnit(unknownUnit);
		access.addTeam("iit");
		Assert.assertTrue(AccessControl.hasAccess(
			iitAlumniAndBizosys, access.getAccessList()));

		access.clear();
		access.addOrgUnit("bizosys");
		access.addTeam(unknownTeam);
		Assert.assertTrue(AccessControl.hasAccess(
			iitAlumniAndBizosys, access.getAccessList()));

		access.clear();
		access.addOrgUnit(unknownUnit);
		access.addOrgUnit("bizosys");
		access.addTeam(unknownTeam);
		Assert.assertTrue(AccessControl.hasAccess(
			iitAlumniAndBizosys, access.getAccessList()));

		access.clear();
		access.addOrgUnit(unknownUnit);
		access.addTeam("iit");
		access.addTeam(unknownTeam);
		Assert.assertTrue(AccessControl.hasAccess(
			iitAlumniAndBizosys, access.getAccessList()));

		/**
		 * False
		 */
		access.clear();
		access.addOrgUnit("iit");
		access.addTeam("bizosys");
		Assert.assertFalse(AccessControl.hasAccess( //Reversed
			iitAlumniAndBizosys, access.getAccessList()));

		access.clear();
		access.addOrgUnit("iit");
		access.addTeam(unknownTeam);
		Assert.assertFalse(AccessControl.hasAccess(
			iitAlumniAndBizosys, access.getAccessList()));

		access.clear();
		Assert.assertFalse(AccessControl.hasAccess(
			new WhoAmI(unknownUnit,unknownTeam), access.getAccessList()));
		
	}
}
