package com.bizosys.hsearch.inpipe;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.br.BrazilianAnalyzer;
import org.apache.lucene.analysis.cn.ChineseAnalyzer;
import org.apache.lucene.analysis.cz.CzechAnalyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.el.GreekAnalyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.nl.DutchAnalyzer;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.th.ThaiAnalyzer;
import org.apache.oneline.ApplicationFault;
import org.apache.oneline.SystemFault;
import org.apache.oneline.conf.Configuration;
import org.apache.oneline.pipes.PipeIn;

import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.TermStream;
import com.bizosys.hsearch.inpipe.util.ReaderType;
import com.bizosys.hsearch.util.LuceneConstants;

public class TokenizeNonEnglish extends TokenizeBase implements PipeIn {

	public Map<String, Analyzer> languageMap = new HashMap<String, Analyzer>();
	
	public TokenizeNonEnglish() {
		super();
	}
	
	public PipeIn getInstance() {
		return this;
	}

	public String getName() {
		return "TokenizeNonEnglish";
	}

	public boolean init(Configuration conf) throws ApplicationFault,SystemFault {
		languageMap.put("br", new BrazilianAnalyzer(LuceneConstants.version));
		languageMap.put("vn", new ChineseAnalyzer());
		languageMap.put("cz", new CzechAnalyzer(LuceneConstants.version));
		languageMap.put("nl", new DutchAnalyzer(LuceneConstants.version));
		languageMap.put("fr", new FrenchAnalyzer(LuceneConstants.version));
		languageMap.put("de", new GermanAnalyzer(LuceneConstants.version));
		languageMap.put("el", new GreekAnalyzer(LuceneConstants.version));
		languageMap.put("ru", new RussianAnalyzer(LuceneConstants.version));
		languageMap.put("th", new ThaiAnalyzer(LuceneConstants.version));
		return true;
	}

	public boolean visit(Object docObj) throws ApplicationFault, SystemFault {
		
		if ( null == docObj) return false;
		Doc doc = (Doc) docObj;
		
		String lang = doc.meta.locale.getDisplayLanguage();
		
		List<ReaderType> readers = super.getReaders(doc);
    	if (null == readers) return true;
		
		try {
	    	for (ReaderType reader : readers) {
	    		Analyzer analyzer = languageMap.get(lang);
	    		if ( null == analyzer) throw new ApplicationFault("TokenizeNonEnglish : Unsupported Lanugage > " + lang );
	    		TokenStream stream = analyzer.tokenStream(reader.type, reader.reader);
	    		TermStream ts = new TermStream(
		    			reader.docSection, stream, reader.type); 
	    		doc.terms.addTokenStream(ts);
	    		//Note : The reader.reader stream is already closed.
			}
	    	return true;
    	} catch (Exception ex) {
    		throw new ApplicationFault(ex);
    	}
	}

	public boolean commit() throws ApplicationFault, SystemFault {
		return true;
	}
}
