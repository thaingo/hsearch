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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.bizosys.hsearch.hbase.HWriter;
import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.IdMapping;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;

/**
 * Delete document from preview and detail record
 * @author karan
 *
 */
public class DeleteFromPreviewAndDetail implements PipeIn {

	List<byte[]> ids = new ArrayList<byte[]>(3);
	public DeleteFromPreviewAndDetail() {
		
	}
	
	public boolean visit(Object objDoc) throws ApplicationFault, SystemFault {
		
		if ( null == objDoc) return true;
		Doc doc = (Doc) objDoc;
		
		if ( null != doc.bucketId && null != doc.docSerialId ) {
			ids.add(IdMapping.getKey(doc.bucketId, doc.docSerialId).getBytes());
			return true;
		} else {
			InpipeLog.l.warn("DeleteFromPreviewAndDetail: Document Original Id or Bucket Id with doc serial is absent.");
			throw new ApplicationFault ("DeleteFromPreviewAndDetail: No Ids provided for deletion.");
		}
	}

	/**
	 * Creating the term bucket to save the changes.
	 */
	public boolean commit() throws ApplicationFault, SystemFault {

		if ( 0 == this.ids.size()) return true;

		try {
			HWriter.delete(IOConstants.TABLE_CONTENT, ids);
			HWriter.delete(IOConstants.TABLE_PREVIEW, ids);
		} catch (IOException ex) {
			StringBuilder sb = new StringBuilder(100);
			sb.append("DeleteFromPreviewAndDetail: Failed Ids are = ");
			for (byte[] id : this.ids) {
				sb.append('[').append(new String(id)).append("] ");
			}
			throw new SystemFault(sb.toString(), ex);
		}
		return true;
	}

	public boolean init(Configuration conf) {
		return true;
	}

	public PipeIn getInstance() {
		return new DeleteFromPreviewAndDetail();
	}

	public String getName() {
		return "DeleteFromPreviewAndDetail";
	}

}
