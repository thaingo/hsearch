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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import junit.framework.TestCase;

import com.bizosys.ferrari.TestFerrari;
import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.index.DocAcl;
import com.bizosys.hsearch.index.DocMeta;

public class FilterMetaAndAclTest extends TestCase {

	public static void main(String[] args) throws Exception {
		FilterMetaAndAclTest t = new FilterMetaAndAclTest();
        TestFerrari.testRandom(t);
	}
	
	public void testAccessSerialization() throws Exception {
		Access acl = new Access();
		acl.addOrgUnit("com.si");
		AccessStorable viewA = acl.toStorable();
		FilterMetaAndAcl fma = new FilterMetaAndAcl(
			viewA,null,null,null,-1L,-1L,-1L,-1L);

		FilterMetaAndAcl fmaN = new FilterMetaAndAcl();
		fmaN.bytesA = fma.bytesA;
		fmaN.deserialize();
	}
	
	public void testCreationSerialization() throws Exception {
		FilterMetaAndAcl fma = new FilterMetaAndAcl(
			null,null,null,null,new Long(System.currentTimeMillis()),1000,-1L,-1L);

		FilterMetaAndAcl fmaN = new FilterMetaAndAcl();
		fmaN.bytesA = fma.bytesA;
		fmaN.deserialize();
		
		assertTrue(  System.currentTimeMillis() - fmaN.createdBefore < 60000 );
		assertEquals(1000, fmaN.createdAfter );
		assertEquals(-1,  fmaN.modifiedAfter );
		assertEquals(-1,  fmaN.modifiedBefore  );
	}

	public void testModifiedSerialization() throws Exception {
		FilterMetaAndAcl fma = new FilterMetaAndAcl(
			null,null,null,null,-1L,-1L,new Long(System.currentTimeMillis()),1000);

		FilterMetaAndAcl fmaN = new FilterMetaAndAcl();
		fmaN.bytesA = fma.bytesA;
		fmaN.deserialize();
		
		assertEquals(-1,  fmaN.createdAfter);
		assertEquals(-1,  fmaN.createdBefore  );
		assertTrue(  System.currentTimeMillis() - fmaN.modifiedBefore < 60000 );
		assertEquals(1000, fmaN.modifiedAfter );
	}
	
	public void testAllSerialization(String role, String keyword, String state, 
			String tenant, Long cb, Long ca, Long mb, Long ma) throws Exception {
		
		Access acl = new Access();
		acl.addRole(role);
		AccessStorable viewA = acl.toStorable();
		FilterMetaAndAcl fma = new FilterMetaAndAcl(
			viewA,new Storable(keyword).toBytes(), new Storable(state).toBytes(),
			new Storable(tenant).toBytes(), cb,ca,mb,ma);

		FilterMetaAndAcl fmaN = new FilterMetaAndAcl();
		fmaN.bytesA = fma.bytesA;
		fmaN.deserialize();
		assertEquals(keyword,  new String(fmaN.keyword));
		assertEquals(tenant,  new String(fmaN.tenant));
		assertEquals(state,  new String(fmaN.state));
		assertEquals(ca.longValue(),  fmaN.createdAfter);
		assertEquals(cb.longValue(),  fmaN.createdBefore  );
		assertEquals(ma.longValue(),  fmaN.modifiedAfter );
		assertEquals(mb.longValue(),  fmaN.modifiedBefore );
	}
	
	public void testFilterAcl(String aUnit, String anotherUnit) throws Exception {
		
		Access acl = new Access();
		acl.addOrgUnit(aUnit);
		AccessStorable viewA = acl.toStorable();
		
		FilterMetaAndAcl fma = new FilterMetaAndAcl(
			viewA,null,null,null,-1L,-1L,-1L,-1L);
		fma.deserialize();
		

		Access va = new Access();
		va.addOrgUnit(aUnit);
		DocAcl docAcl = new DocAcl(va,null);  
		assertTrue( fma.allowAccess(docAcl.toBytes()) );
		
		Access vb = new Access();
		vb.addOrgUnit(anotherUnit);
		assertFalse( fma.allowAccess(new DocAcl(vb,null).toBytes()) );
	}
	
	public void testFilterMeta(String keyword, String state, 
			String tenant, Long cb, Long ca, Long mb, Long ma) throws Exception {
		if ( cb < 0) cb = cb * -1;
		if ( ca < 0) ca = ca * -1;
		if ( mb < 0) mb = mb * -1;
		if ( ma < 0) ma = ma * -1;
		
		if ( cb > ca) {
			long temp = cb;
			cb = ca;
			ca = temp;
		}
		
		if ( mb > ma) {
			long temp = mb;
			mb = ma;
			ma = temp;
		}

		FilterMetaAndAcl fma = new FilterMetaAndAcl(
			null,new Storable(keyword).toBytes(), new Storable(state).toBytes(),
			new Storable(tenant).toBytes(), cb,ca,mb,ma);
		fma.deserialize();
		
		DocMeta  dm = new DocMeta();
		dm.state =  state;
		dm.tenant =  tenant;
		List<String> tagL = new ArrayList<String>();
		tagL.add(keyword);
		tagL.add("ram");
		dm.addTags(tagL);
		dm.createdOn =  new  Date( cb + ( (ca-cb) / 2) );
		dm.modifiedOn =  new  Date( mb + ( (ma-mb) / 2) );
		assertTrue(fma.allowMeta(dm.toBytes()));
		
	}
	
}
