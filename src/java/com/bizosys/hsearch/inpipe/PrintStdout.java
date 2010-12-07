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

import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;

import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.DocTeaser;
import com.bizosys.hsearch.index.DocTerms;
import com.bizosys.hsearch.index.Term;

/**
 * Print the indexing details to console
 * @author karan
 *
 */
public class PrintStdout implements PipeIn {

	public boolean commit() throws ApplicationFault, SystemFault {
		return true;
	}

	public PipeIn getInstance() {
		return this;
	}

	public String getName() {
		return "PrintStdout";
	}

	public boolean init(Configuration conf) throws ApplicationFault, SystemFault {
		return true;
	}

	public boolean visit(Object docObj) throws ApplicationFault, SystemFault {
		if ( null == docObj) return false;
		Doc doc = (Doc) docObj;
		
    	DocTeaser teaser = doc.teaser;
    	if ( null != teaser) {
    		System.out.println("Url:" + teaser.getUrl() );
    		System.out.println("Title :" + teaser.getTitle() );
    		System.out.println("Preview :" + teaser.getPreview() );
    		System.out.println("Cache :" + teaser.getCachedText() );
    	}
    	DocTerms terms = doc.terms;
    	StringBuilder sb = new StringBuilder();
    	if ( null != terms) {
    		if ( null != terms.all) {
    			for (Term term: terms.all) {
    				sb.append("Term [").append(term.toString()).append("]\n");
				}
    		}
    	}
    	System.out.println("Terms :" + sb.toString() );

    	return true;
	}
	
}
