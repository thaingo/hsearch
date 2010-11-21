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
package com.bizosys.hsearch.hbase;

import com.bizosys.hsearch.common.IStorable;
import com.bizosys.hsearch.common.Storable;

public class NV {

	public IStorable family = null;
	public IStorable name = null;
	public IStorable data = null;

	public NV(String family, String name) {
		this.family = new Storable(family);
		this.name = new Storable(name);
	}
	
	public NV(byte[] family, byte[] name) {
		this.family = new Storable(family);
		this.name = new Storable(name);
	}
	
	public NV(byte[] family, byte[] name, IStorable data) {
		this.family = new Storable(family);
		this.name = new Storable(name);
		this.data = data;
	}
	
	public NV(String family, String name, IStorable data ) {
		this.family = new Storable(family);
		this.name = new Storable(name);
		this.data = data;
	}

	public NV(IStorable family, IStorable name, IStorable data ) {
		this.family = family;
		this.name = name;
		this.data = data;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder(50);
		sb.append("  F:[").append(new String(family.toBytes())).
		append("] N:[").append(new String(name.toBytes())).
		append("] D:").append(data.toString());
		return sb.toString();
	}
}
