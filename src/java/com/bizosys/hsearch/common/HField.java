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

import java.util.Date;

import com.bizosys.oneline.ApplicationFault;

public class HField implements Field {
	
	public boolean isIndexable = true;
	public boolean isAnalyzed = true;
	public boolean isStored = true;
	
	public ByteField bfl = null;
	
	public HField(boolean isIndexable,
		boolean isAnalyzed,boolean isStored ) {
		
		this.isIndexable = isIndexable;
		this.isAnalyzed = isAnalyzed;
		this.isStored = isStored;
	}
	
	public HField(String name, String value) {
		this.bfl = new ByteField(name, value);
		bfl.enableTypeOnToBytes(true);
	}

	public HField(String name, long value) {
		this.bfl = new ByteField(name, value);
		bfl.enableTypeOnToBytes(true);
	}
	
	public HField(String name, byte value) {
		this.bfl = new ByteField(name, value);
		bfl.enableTypeOnToBytes(true);
	}

	public HField(String name, boolean value) {
		this.bfl = new ByteField(name, value);
		bfl.enableTypeOnToBytes(true);
	}

	public HField(String name, short value) {
		this.bfl = new ByteField(name, value);
		bfl.enableTypeOnToBytes(true);
	}
	
	public HField(String name, int value) {
		this.bfl = new ByteField(name, value);
		bfl.enableTypeOnToBytes(true);
	}

	public HField(String name, float value) {
		this.bfl = new ByteField(name, value);
		bfl.enableTypeOnToBytes(true);
	}

	public HField(String name, double value) {
		this.bfl = new ByteField(name, value);
		bfl.enableTypeOnToBytes(true);
	}

	public HField(String name, Date value) {
		this.bfl = new ByteField(name, value);
		bfl.enableTypeOnToBytes(true);
	}

	public HField(String name, byte[] value) {
		this.bfl = new ByteField(name, value);
		bfl.enableTypeOnToBytes(true);
	}

	public ByteField getByteField() throws ApplicationFault {
		return this.bfl;
	}

	public boolean isAnalyze() {
		return this.isAnalyzed;
	}

	public boolean isIndexable() {
		return this.isIndexable;
	}

	public boolean isStore() {
		return this.isStored;
	}
}
