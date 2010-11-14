package com.bizosys.hsearch.security;

import org.apache.oneline.ApplicationFault;
import org.apache.oneline.SystemFault;
import org.apache.oneline.util.StringUtils;

import com.bizosys.hsearch.common.Access;
import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.common.StorableList;

public class AccessControl {
	
	/**
	 * Form an access object out of a WhoAmI string
	 * @param whoami
	 * @return
	 */
	public static final Access getAccessControl(WhoAmI whoami) {
		
		if ( null == whoami) return null;
		Access acl = new Access();
				
		boolean hasRoles = (null != whoami.roles);
		if ( hasRoles ) {
			for (String role : whoami.roles) {
				if ( ! StringUtils.isEmpty(role) ) acl.addRole(role);
			}
		}

		if ( null != whoami.teams ) {
			for (String team : whoami.teams) {
				if ( ! StringUtils.isEmpty(team) ) acl.addTeam(team); 
			}
		}
		
		boolean hasUid = ! StringUtils.isEmpty( whoami.uid);
		if ( hasUid ) acl.addUid(whoami.uid); 
		boolean hasOu = ! StringUtils.isEmpty( whoami.ou);
		if ( hasOu ) acl.addOrgUnit(whoami.ou); 
		if ( hasUid && hasOu ) acl.addOrgUnitAndUid(whoami.ou, whoami.uid);
		if ( hasOu && hasRoles ) {
			for (String role : whoami.roles) {
				if ( ! StringUtils.isEmpty(role) ) acl.addOrgUnitAndRole(whoami.ou, role);
			}
		}
		
		return acl;
	}
	
	/**
	 * Check for the available access for a user against the given access
	 * @param whoami
	 * @param access
	 * @return
	 * @throws ApplicationFault
	 * @throws SystemFault
	 */
	public static boolean hasAccess (WhoAmI whoami, StorableList access) 
		throws ApplicationFault, SystemFault {

		Access acl = AccessControl.getAccessControl(whoami);
		StorableList userAcls = acl.getAccessList();

		boolean allow = false;
		
		for (Object objFoundAcl : access) {
			byte[] foundAcl =  ((byte[]) objFoundAcl);
			
			if (Storable.compareBytes(foundAcl, Access.ANY_BYTES)) {
				allow = true; break;
			}
			
			for (Object userAcl : userAcls) {
				allow = Storable.compareBytes(foundAcl, (byte[]) userAcl);
				if ( allow ) break;
			}
			if ( allow ) break;
		}
		return allow;
	}
}
