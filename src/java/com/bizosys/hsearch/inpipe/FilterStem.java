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

import java.util.List;

import org.apache.lucene.analysis.TokenStream;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;

import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.DocTerms;
import com.bizosys.hsearch.index.TermStream;
import com.bizosys.hsearch.inpipe.util.StemFilterWrap;

/**
 * Stem the terms
 * @author karan
 *
 */
public class FilterStem implements PipeIn {
	
	boolean isStemming = true;
	
	public FilterStem() {}

	public boolean commit() throws ApplicationFault, SystemFault {
		return true;
	}

	public PipeIn getInstance() {
		return this;
	}

	public String getName() {
		return "FilterStem";
	}

	public boolean init(Configuration conf) throws ApplicationFault, SystemFault {
		this.isStemming = conf.getBoolean("stemming", true);
		return true;
	}

	public boolean visit(Object docObj) throws ApplicationFault, SystemFault {
		if ( ! this.isStemming ) return true;
		
		if ( null == docObj) return false;
		Doc doc = (Doc) docObj;
		DocTerms terms = doc.terms;
		if ( null == terms) return false;
		
		List<TermStream> streams = terms.getTokenStreams();
		if ( null == streams) return true; //Allow for no bodies
		
		for (TermStream ts : streams) {
			TokenStream stream = ts.stream;
			if ( null == stream) continue;
			stream = new StemFilterWrap(stream);
			ts.stream = stream;
		}
		return true;
	}
	
}
