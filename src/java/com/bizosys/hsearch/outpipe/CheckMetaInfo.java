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
package com.bizosys.hsearch.outpipe;

import java.util.List;

import com.bizosys.hsearch.query.DocMetaWeight;
import com.bizosys.hsearch.query.HQuery;
import com.bizosys.hsearch.query.QueryContext;
import com.bizosys.hsearch.query.QueryResult;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeOut;

/**
 * Filters and Ranks on Meta information
 * @author karan
 *
 */
public class CheckMetaInfo implements PipeOut{
	
	int DEFAULT_RETRIEVAL_SIZE = 100;
	
	public CheckMetaInfo() {
	}	

	public boolean visit(Object objQuery) throws ApplicationFault, SystemFault {
		HQuery query = (HQuery) objQuery;
		QueryResult result = query.result;
		QueryContext ctx = query.ctx;
		if ( null == result) return true;
		
		Object[] staticL = result.sortedStaticWeights;
		if ( null == staticL) return true;
		
		CheckMetaInfoHBase hbase = new CheckMetaInfoHBase(ctx);
		int pageSize = (-1 == ctx.metaFetchLimit) ? 
			DEFAULT_RETRIEVAL_SIZE : ctx.metaFetchLimit;
		List<DocMetaWeight> dmwL = hbase.filter(staticL, ctx.scroll, pageSize);
		if ( null == dmwL) return true;
		result.sortedDynamicWeights = dmwL.toArray();
		return true;
	}
	
	public boolean commit() throws ApplicationFault, SystemFault {
		return true;
	}

	public PipeOut getInstance() {
		return this;
	}

	public boolean init(Configuration conf) throws ApplicationFault, SystemFault {
		this.DEFAULT_RETRIEVAL_SIZE = conf.getInt("meta.fetch.limit", 100);
		return true;
	}
	
	public String getName() {
		return "CheckMetaInfo";
	}			
}
