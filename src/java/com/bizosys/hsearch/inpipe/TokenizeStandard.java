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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;

import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.DocTerms;
import com.bizosys.hsearch.index.TermStream;
import com.bizosys.hsearch.inpipe.util.ReaderType;
import com.bizosys.hsearch.util.LuceneConstants;

/**
 * Standard Tokenizer
 * @author karan
 *
 */
public class TokenizeStandard extends TokenizeBase implements PipeIn {

	public TokenizeStandard() {
		super();
	}
	
	public PipeIn getInstance() {
		return this;
	}

	public String getName() {
		return "TokenizeStandard";
	}

	public boolean init(Configuration conf) throws ApplicationFault,SystemFault {
		return true;
	}

	public boolean visit(Object docObj) throws ApplicationFault, SystemFault {
		if ( null == docObj) return false;
		Doc doc = (Doc) docObj;
		List<ReaderType> readers = super.getReaders(doc);
    	if (null == readers) return true;
		
		try {
	    	for (ReaderType reader : readers) {
				if ( null == doc.terms) doc.terms = new DocTerms();
	    		Analyzer analyzer = new StandardAnalyzer(LuceneConstants.version);
	    		TokenStream stream = analyzer.tokenStream(
	    				reader.type, reader.reader);
	    		TermStream ts = new TermStream(
		    			reader.docSection, stream, reader.type); 
		    	doc.terms.addTokenStream(ts);
	    		//Note : The reader.reader stream is already closed.
			}
	    	return true;
    	} catch (Exception ex) {
    		throw new SystemFault(ex);
    	}
	}

	public boolean commit() throws ApplicationFault, SystemFault {
		return true;
	}
}
