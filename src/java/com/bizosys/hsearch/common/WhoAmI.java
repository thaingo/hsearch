/*
* Copyright 2010 The Apache Software Foundation
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.bizosys.hsearch.common;

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

	public WhoAmI (String uid){
		this.uid = uid;
	}

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
