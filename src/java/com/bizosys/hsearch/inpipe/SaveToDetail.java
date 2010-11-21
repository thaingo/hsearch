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
package com.bizosys.hsearch.inpipe;

import java.util.ArrayList;
import java.util.List;

import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;

import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.hbase.HWriter;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.IdMapping;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.hsearch.util.Record;

public class SaveToDetail implements PipeIn {

	List<Doc> details = new ArrayList<Doc>();
	public SaveToDetail() {
		
	}
	
	public boolean visit(Object objDoc) throws ApplicationFault, SystemFault {
		
		if ( null == objDoc) return true;
		Doc doc = (Doc) objDoc;

		if ( null == doc.content) return true;
		
		if ( null == details) details = new ArrayList<Doc>();
		this.details.add(doc);
		return true;
	}

	/**
	 * Creating the term bucket to save the changes.
	 */
	public boolean commit() throws ApplicationFault, SystemFault {

		if ( null == this.details) return true;
		
		/**
		 * Iterate through all content 
		 */
		try {
			List<Record> contentRecords = new ArrayList<Record>(this.details.size()); 
			for (Doc doc : this.details) {
				if ( null == doc.content ) continue;
				String id = IdMapping.getKey(doc.bucketId, doc.docSerialId);
				List<NV> nvs = new ArrayList<NV>();
				doc.content.toNVs(nvs);
				if ( nvs.size() > 0 ) contentRecords.add(new Record(new Storable(id),nvs));
			}
			if ( 0 == contentRecords.size()) return true;
			HWriter.insert(IOConstants.TABLE_CONTENT, contentRecords);
		} catch (Exception ex) {
			throw new ApplicationFault("SaveToDetail : Failed", ex);
		}
		return true;
	}

	public boolean init(Configuration conf) throws ApplicationFault, SystemFault {
		return true;
	}

	public PipeIn getInstance() {
		return new SaveToDetail();
	}

	public String getName() {
		return "SaveToDetail";
	}

}
