package com.bizosys.hsearch.security;

/**
 * This is the user based on which complete security framework is designed.
 * This user will propagate from the parent application
 * @author karan
 *
 */
public class WhoAmI 
{
	
	public String uid = null;

	/**
	 * Common Name : Abinasha Karana
	 */
	public String cn = null;
	
	/**
	 * o: bizosys.com
	 */
	public String o = null;

	/**
	 * dc: bizosys
	 */
	public String dc = null;

	/**
	 * Organization Unit : IT, SALES
	 */
	public String ou = null;

	/**
	 * Roles , Multiple Roles Supported
	 */
	public String[] roles = null;

	/**
	 * Which all teams it belongs to
	 */
	public String[] teams = null;

	/**
	 * The Super-User's node
	 */
	public WhoAmI manager = null;
	
	
	public String hexdigest = null;
	
	public WhoAmI (){}

	public WhoAmI (String ou, String role ){
		this.ou = ou;
		this.roles = new String[] {role};
	}
	
	public WhoAmI (String ou, String[] roles ){
		this.ou = ou;
		this.roles = roles;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(100);
		sb.append(" uid : ").append(this.uid).append('\n');
		sb.append(" cn : ").append(this.cn).append('\n');
		sb.append(" o : ").append(this.o).append('\n');
		sb.append(" dc : ").append(this.dc).append('\n');
		sb.append(" ou : ").append(this.ou).append('\n');
		sb.append(" teams : ").append(this.teams).append('\n');
		sb.append(" roles : ").append(this.roles).append('\n');
		sb.append(" manager:").append(this.manager).append('\n');
		return sb.toString();
	}

}
