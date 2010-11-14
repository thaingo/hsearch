package com.bizosys.hsearch.inpipe;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.oneline.ApplicationFault;
import org.apache.oneline.util.StringUtils;

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

public abstract class TokenizeBase {
	
	/**
	 * Pack different sections with different readers.
	 * This potentially helps on weight assignment.
	 * @param aDocument
	 * @return
	 */
	protected List<ReaderType> getReaders(Doc aDocument) throws ApplicationFault {
		
		List<ReaderType> readers = new ArrayList<ReaderType>();
		DocTeaser teaser = aDocument.teaser;
		DocContent content = aDocument.content;
		DocMeta meta = aDocument.meta;
		DocTerms terms = aDocument.terms;
		
		/**
		 * The content fields
		 */
		if ( null != content.analyzedIndexed) 
			addReader(content.analyzedIndexed, terms,  readers, Term.TERMLOC_XML,true);
		if ( null != content.nonAnalyzedIndexed) 
			addReader(content.nonAnalyzedIndexed, terms,  readers, Term.TERMLOC_XML,false);

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
		if ( null != meta.authorKeywords || null != meta.readerKeywords) {
			List<ByteField> keywords = new ArrayList<ByteField>();
			if ( null != meta.authorKeywords ){
				for (String keyword : meta.authorKeywords) {
					keywords.add(new ByteField(TermType.KEYWORD, keyword));
				}
			}
			
			if ( null != meta.readerKeywords ){
				for (String keyword : meta.readerKeywords) {
					keywords.add(new ByteField(TermType.KEYWORD, keyword));
				}
			}
			
			addReader(keywords,terms,readers,Term.TERMLOC_KEYWORD, false);
		}
		
		return readers;
	}

	private void addReader(List<ByteField> fields, DocTerms terms,  
		List<ReaderType> readers, Character termLoc, boolean analyze) throws ApplicationFault {
		
		for (ByteField fld: fields) {
			addReader(fld, terms, readers, termLoc, analyze);
		}
	}
	
	private void addReader(ByteField fld, DocTerms terms,
		List<ReaderType> readers, Character termLoc, boolean analyze) throws ApplicationFault {
		
		if (fld.type == Storable.BYTE_STRING) {
			Object objStr = fld.getValue();
			if ( null == objStr) return;
			String text = (String) objStr;
			text = text.toLowerCase();
			
			boolean oneWord = text.indexOf(' ') < 0 ;
			if (oneWord || !analyze) {
				Term term = new Term(text.toLowerCase(),termLoc,fld.name,0);
				terms.getTermList().add(term);
			} else { 
				InputStream ba = new ByteArrayInputStream( fld.toBytes());
				InputStreamReader is = new InputStreamReader(ba);   
				readers.add(new ReaderType(termLoc,fld.name,is));
			} 							
		} else {
			//TODO :: Enable Fields of Type other than String 
		}
	}
}
