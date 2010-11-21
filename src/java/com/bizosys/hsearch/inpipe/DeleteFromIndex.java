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

import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.hbase.HWriter;
import com.bizosys.hsearch.hbase.IUpdatePipe;
import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.schema.ILanguageMap;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;

public class DeleteFromIndex implements PipeIn {

	List<Doc> documents = new ArrayList<Doc>();
	
	public DeleteFromIndex() {
	}
	
	public DeleteFromIndex(int docMergeFactor) {
	}

	public boolean visit(Object objDoc) throws ApplicationFault, SystemFault {
		if ( null == objDoc) return false;
		if ( null == documents) documents = new ArrayList<Doc>();
		documents.add((Doc)objDoc);
		return true;
	}

	/**
	 * Cuts out section of docpositions which are in the removal list.
	 */
	public boolean commit() throws ApplicationFault, SystemFault {
		Doc curDoc = null; 
		try {
			for (Doc aDoc : documents) {
				curDoc = aDoc;
				IUpdatePipe pipe = new DeleteFromIndexWithCut(aDoc.docSerialId);
				byte[] pk = Storable.putLong(aDoc.bucketId);
				for (Character c : ILanguageMap.ALL_TABLES) {
					String t = c.toString();
					HWriter.update(t, pk, pipe);
				}
			}
			return true;
		} catch (Exception ex) {
			if ( null != curDoc) throw new SystemFault(curDoc.toString(), ex);
			else throw new SystemFault(ex);
		}
	}

	public boolean init(Configuration conf) throws ApplicationFault, SystemFault {
		return true;
	}

	public PipeIn getInstance() {
		return this;
	}

	public String getName() {
		return "DeleteFromIndex";
	}
}
