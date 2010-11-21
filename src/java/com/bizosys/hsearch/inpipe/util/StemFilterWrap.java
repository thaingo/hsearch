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
package com.bizosys.hsearch.inpipe.util;

import java.io.IOException;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

import com.bizosys.hsearch.lang.Stemmer;

public class StemFilterWrap extends TokenFilter {

	private Stemmer stemmer;
	private TermAttribute termA = null; 
	

	public StemFilterWrap(TokenStream in) {
		super(in);
		stemmer = Stemmer.getInstance();
		this.termA = (TermAttribute)in.getAttribute(TermAttribute.class);
	}
	
	public final boolean incrementToken() throws IOException {
		boolean isIncremented = input.incrementToken();
		if ( ! isIncremented ) return isIncremented;
		
		 if (termA != null) {
			 String stemWord = stemmer.stem(termA.term());
			 this.termA.setTermBuffer(stemWord);
		}
		 return isIncremented;
	}
	
	
}
