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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;

import com.bizosys.hsearch.index.Doc;

/**
 * Remove non-ascii words from the content
 * @author karan
 *
 */
public class RemoveNonAscii implements PipeIn {

	Pattern pattern = null;
	String replaceStr = " ";


	public boolean commit() throws ApplicationFault, SystemFault {
		return true;
	}

	public PipeIn getInstance() {
		return this;
	}

	public String getName() {
		return "RemoveNonAscii";
	}

	public boolean init(Configuration conf) throws ApplicationFault, SystemFault {
		this.pattern = Pattern.compile("[^\\p{ASCII}]");
		return true;
	}

	public boolean visit(Object docObj) throws ApplicationFault, SystemFault {
		if ( null == docObj) return false;
		Doc doc = (Doc) docObj;
		if ( null != doc.teaser) doc.teaser.cacheText = null;
		
		if ( null != doc.teaser.title ) {
			Matcher titleMatcher = pattern.matcher(doc.teaser.getTitle());
			doc.teaser.setTitle(titleMatcher.replaceAll(replaceStr));
		}

		if ( null != doc.teaser.cacheText ) {
			Matcher cacheTextMatcher = pattern.matcher(doc.teaser.getCachedText());
			doc.teaser.setTitle(cacheTextMatcher.replaceAll(replaceStr));
		}
		return true;
	}
	
}
