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

import junit.framework.TestCase;

import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.services.ServiceFactory;

import com.bizosys.ferrari.TestFerrari;
import com.bizosys.hsearch.common.RecordScalar;
import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.hbase.HReader;
import com.bizosys.hsearch.hbase.HWriter;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.schema.IOConstants;

public class HReaderTest extends TestCase {

	public static void main(String[] args) throws Exception {
		HReaderTest t = new HReaderTest();
		Configuration conf = new Configuration();
		ServiceFactory.getInstance().init(conf, null);
        TestFerrari.testRandom(t);
	}
	
	public void testKeyGeneration() throws Exception {
		NV nv = new NV(IOConstants.NAME_VALUE_BYTES,IOConstants.NAME_VALUE_BYTES);
		byte[] pk = "BUCKET_COUNTER".getBytes();		
		RecordScalar scalar = new RecordScalar(pk, nv);
		scalar.pk = new Storable(pk);
		if ( ! HReader.exists(IOConstants.TABLE_CONFIG, pk)) {
			HWriter.insertScalar(IOConstants.TABLE_CONFIG, scalar);
		}
		long bucketId = HReader.generateKeys(IOConstants.TABLE_CONFIG,scalar,1);
		System.out.println(bucketId);
	}
}
