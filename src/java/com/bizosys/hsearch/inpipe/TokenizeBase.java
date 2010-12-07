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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import com.bizosys.hsearch.common.ByteField;
import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.DocContent;
import com.bizosys.hsearch.index.DocMeta;
import com.bizosys.hsearch.index.DocTeaser;
import com.bizosys.hsearch.index.DocTerms;
import com.bizosys.hsearch.index.Term;
import com.bizosys.hsearch.index.TermType;
import com.bizosys.hsearch.inpipe.util.ReaderType;
import com.bizosys.hsearch.lang.Stemmer;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.util.StringUtils;

/**
 * This is an abstract class which reads the various dimensions of 
 * the document and tokenizes them including ID, URL, Fields, Title. 
 * @author karan
 *
 */
public abstract class TokenizeBase {
	
	/**
	 * Pack different sections with different readers.
	 * This potentially helps on weight assignment.
	 * @param aDocument	A document
	 * @return	Reader types
	 */
	protected List<ReaderType> getReaders(Doc aDocument) 
	throws SystemFault, ApplicationFault {
		
		List<ReaderType> readers = new ArrayList<ReaderType>();
		DocTeaser teaser = aDocument.teaser;
		DocContent content = aDocument.content;
		DocMeta meta = aDocument.meta;
		if ( null == aDocument.terms) aDocument.terms = new DocTerms(); 
		DocTerms terms = aDocument.terms;
		
		if ( null != content) { //The content fields
			if ( null != content.analyzedIndexed) 
				addReader(content.analyzedIndexed, terms,  readers, Term.TERMLOC_XML,true);
			if ( null != content.nonAnalyzedIndexed) 
				addReader(content.nonAnalyzedIndexed, terms,  readers, Term.TERMLOC_XML,false);
		}

		/**
		 * Add the non analyzed ID field
		 */
		if ( null != teaser.id) {
			addReader(new ByteField(TermType.URL_OR_ID, teaser.id), terms, readers, Term.TERMLOC_URL, false);
		}
		
		/**
		 * Adding the URL first
		 */
		String url = teaser.getUrl();
		if ( ! StringUtils.isEmpty(url) ) {
			if ( url.startsWith("http") || url.startsWith("file") ) {
				url = StringUtils.replaceMultipleCharsToAnotherChar(
					url, new char[]{'-','_','/','.','?','&','='}, ' ');
			    StringTokenizer tokenizer = new StringTokenizer (url," ");
				List<ByteField> fields = new ArrayList<ByteField>(); 
			    while (tokenizer.hasMoreTokens()) {
			    	fields.add(new ByteField(TermType.URL_OR_ID, tokenizer.nextToken()));
			    }
				addReader(fields, terms,  readers, Term.TERMLOC_URL, true);
			}
		}

		/**
		 * Adding the title
		 */
		if ( null != teaser.getTitle()) {
			ByteField title = new ByteField(TermType.TITLE, teaser.getTitle());
			addReader(title,terms,readers,Term.TERMLOC_SUBJECT, true);
		}
		
		/**
		 * Adding the cached text
		 */
		if ( null != teaser.getCachedText()) {
			ByteField cached = new ByteField(TermType.BODY, teaser.getCachedText());
			addReader(cached,terms,readers,Term.TERMLOC_BODY, true);
		}
		
		/**
		 * Adding the Keywords
		 */
		if ( null != meta.tags || null != meta.socialText) {
			List<ByteField> keywords = new ArrayList<ByteField>();
			if ( null != meta.tags ){
				for (String keyword : meta.getTags()) {
					keywords.add(new ByteField(TermType.KEYWORD, keyword));
				}
			}
			
			if ( null != meta.socialText ){
				for (String keyword : meta.getSocialText()) {
					keywords.add(new ByteField(TermType.KEYWORD, keyword));
				}
			}
			
			addReader(keywords,terms,readers,Term.TERMLOC_KEYWORD, false);
		}
		
		return readers;
	}

	private void addReader(List<ByteField> fields, DocTerms terms,  
		List<ReaderType> readers, Character termLoc, boolean analyze) 
	throws SystemFault, ApplicationFault  {
		
		for (ByteField fld: fields) {
			addReader(fld, terms, readers, termLoc, analyze);
		}
	}
	
	private void addReader(ByteField fld, DocTerms terms,
		List<ReaderType> readers, Character termLoc, boolean analyze)
		throws SystemFault, ApplicationFault {
		
		String text = null;
		if (fld.type == Storable.BYTE_STRING) {
			Object objStr = fld.getValue();
			if ( null == objStr) return;
			text = (String) objStr;
		} else if (fld.type == Storable.BYTE_STORABLE) {
			text = fld.getValue().toString();
		}

		if ( null == text) throw 
			new SystemFault("TokenizerBase: Unknow data type :" + fld.toString());
		
		text = text.toLowerCase();
		boolean oneWord = text.indexOf(' ') < 0 ;
		if (oneWord || !analyze) {
			text = Stemmer.getInstance().stem(text);
			Term term = new Term(text,termLoc,fld.name,0);
			terms.getTermList().add(term);
		} else { 
			InputStream ba = new ByteArrayInputStream( fld.toBytes());
			InputStreamReader is = new InputStreamReader(ba);   
			readers.add(new ReaderType(termLoc,fld.name,is));
		} 							
	}
}
