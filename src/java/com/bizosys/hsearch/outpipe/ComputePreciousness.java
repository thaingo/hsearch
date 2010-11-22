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

import com.bizosys.hsearch.query.HQuery;
import com.bizosys.hsearch.query.QueryContext;
import com.bizosys.hsearch.query.QueryPlanner;
import com.bizosys.hsearch.query.QueryTerm;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeOut;

public class ComputePreciousness implements PipeOut{
	
	public ComputePreciousness() {
	}	

	public boolean visit(Object objQuery) throws ApplicationFault, SystemFault {
		
		HQuery query = (HQuery) objQuery;
		QueryContext ctx = query.ctx;
		QueryPlanner planner = query.planner;
		
		/**
		 * Go through the list to find which one maximim occuring
		 * Compute based on that from 0-1 scale the preciousness
		 */
		int maxOccurance1 = computeMaximimOccurance(planner.mustTerms);
		int maxOccurance2 = computeMaximimOccurance(planner.optionalTerms);
		int maxOccurance = ( maxOccurance1 > maxOccurance2) ? maxOccurance1 : maxOccurance2;
		if ( 0 == maxOccurance) throw new ApplicationFault(ctx.queryString);
		computePreciousness(planner.mustTerms, maxOccurance);
		computePreciousness(planner.optionalTerms, maxOccurance);
		return true;
	}
	
	/**
	 * Compute the maximum occurance instance.  
	 * @param queryWordL
	 * @return
	 * @throws ApplicationFault
	 */
	private int computeMaximimOccurance(List<QueryTerm> queryWordL) 
	throws ApplicationFault {
		
		int maxOccurance = 0;
		if ( null == queryWordL) return 0;
		for (QueryTerm term : queryWordL) {
			if ( null == term.foundTerm) continue;
			if ( term.foundTerm.fldFreq > maxOccurance)
				maxOccurance = term.foundTerm.fldFreq;
		}
		return maxOccurance;
	}
	
	/**
	 * 1 is most previous and 0 is least precious
	 * Less found terms are more precious in nature 
	 * @param queryWordL
	 * @param maxOccurance
	 * @throws ApplicationFault
	 */
	private void computePreciousness(List<QueryTerm> queryWordL, 
		int maxOccurance) throws ApplicationFault {
		
		if ( null == queryWordL) return;
		for (QueryTerm term : queryWordL) {
			if ( null == term.foundTerm) continue;
			term.preciousNess = 1 - ( term.foundTerm.fldFreq / maxOccurance);
		}
	}

	
	public boolean commit() throws ApplicationFault, SystemFault {
		return true;
	}

	public PipeOut getInstance() {
		return this;
	}

	public boolean init(Configuration conf) throws ApplicationFault, SystemFault {
		return false;
	}
}
