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
package com.bizosys.hsearch.index;

import java.util.ArrayList;
import java.util.List;

public class DocTerms {

	private List<TermStream> tokenStreams = null;
	public List<Term> all = null;

	public List<Term> getTermList() {
		if ( null != all) return all;
		all = new ArrayList<Term>(100);
		return all;
	}
	
	public void setDocumentTypeCode(byte code) {
		if ( null == all) return;
		for (Term term : all) {
			term.setDocumentTypeCode(code);
		}
	}
	
	public List<TermStream> getTokenStreams() {
		return this.tokenStreams;
	}

	/**
	 * At which location this stream was found..
	 * @param name
	 * @param modifiedStream
	 */
	public void addTokenStream(TermStream stream) {
		if ( L.l.isDebugEnabled())
			L.l.debug("DocTerms > Adding token stream - " + stream.sighting + " - " + stream.type);
		if ( null == tokenStreams) 
			tokenStreams = new ArrayList<TermStream>();
		
		tokenStreams.add(stream);		
	}

	/**
	 * Release all the held resources. Recycles this object for the
	 * next processing.
	 */
	public void cleanup() {
		try { 
			if ( null != this.tokenStreams) {
				for (TermStream tstream: this.tokenStreams) {
					tstream.stream.close();
				}
				this.tokenStreams.clear();
			}
		} catch (Exception ex) {
			L.l.warn("DocTerms:cleanup: ", ex);
		}
	}
}
