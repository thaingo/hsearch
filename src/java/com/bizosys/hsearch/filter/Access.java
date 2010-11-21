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