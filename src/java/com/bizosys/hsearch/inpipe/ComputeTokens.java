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
import java.util.List;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;

import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.DocTerms;
import com.bizosys.hsearch.index.DocumentType;
import com.bizosys.hsearch.index.Term;
import com.bizosys.hsearch.index.TermStream;

/**
 * Tokenize text content
 * @author karan
 *
 */
public class ComputeTokens implements PipeIn {

	public boolean commit() {
		return true;
	}

	public PipeIn getInstance() {
		return this;
	}

	public String getName() {
		return "ComputeTokens";
	}

	public boolean init(Configuration conf) {
		return true;
	}

	public boolean visit(Object docObj) throws SystemFault, ApplicationFault {
		if ( null == docObj) return false;
		Doc doc = (Doc) docObj;
		
		List<TermStream> streams = doc.terms.getTokenStreams();
		if ( null != streams) {
			for (TermStream ts : streams) {
				tokenize(doc, ts);
			}
		}
		
		/**
		 * Assign the document type to all the terms
		 */
		Byte docTypeCode = null;
		if ( null != doc.meta.docType) 
			docTypeCode = DocumentType.getInstance().getTypeCode(doc.meta.docType);
		if (null == docTypeCode) return true;
		doc.terms.setDocumentTypeCode(docTypeCode);
		
		return true;
	}
	
	private void tokenize(Doc doc, TermStream ts) throws SystemFault, ApplicationFault {
		if ( null == ts) return;
		TokenStream stream = ts.stream;
		if ( null == stream) return;
		
		DocTerms terms = doc.terms;
		if ( null == doc.terms) {
			terms = new DocTerms();
			doc.terms = terms; 
		}
		
		String token = null;
		int offset = 0;
		try {
			TermAttribute termA = (TermAttribute)stream.getAttribute(TermAttribute.class);
			OffsetAttribute offsetA = (OffsetAttribute) stream.getAttribute(OffsetAttribute.class);
			//TypeAttribute typeA = (TypeAttribute) stream.getAttribute(TypeAttribute.class);
			stream.reset();
			while ( stream.incrementToken()) {
				token = termA.term();
				//type = typeA.type();
				offset = offsetA.startOffset();
				Term term = new Term(token,ts.sighting,ts.type,offset);
				terms.getTermList().add(term);
			}
		} catch (IOException ex) {
			throw new SystemFault ("ComputeTokens : Tokenize Failed for " + token + " at " + offset , ex);
		} finally {
			if ( null != stream ) try { stream.close(); } catch (IOException ex) {};			
		}
	}
	
	
}
