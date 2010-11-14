package com.bizosys.hsearch.inpipe;

import java.text.Normalizer;

import org.apache.oneline.ApplicationFault;
import org.apache.oneline.SystemFault;
import org.apache.oneline.conf.Configuration;
import org.apache.oneline.pipes.PipeIn;

import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.DocTeaser;

public class NormalizeAccents implements PipeIn {

	public boolean commit() throws ApplicationFault, SystemFault {
		return true;
	}

	public PipeIn getInstance() {
		return this;
	}

	public String getName() {
		return "NormalizeAccents";
	}

	public boolean init(Configuration conf) throws ApplicationFault, SystemFault {
		return true;
	}

	public boolean visit(Object docObj) throws ApplicationFault, SystemFault {
		if ( null == docObj) return false;
		Doc doc = (Doc) docObj;
		
    	DocTeaser teaser = doc.teaser;
    	if ( null != teaser) {
    		String titleText = teaser.getTitle();
    		if ( null != titleText) teaser.setTitle(
    			Normalizer.normalize(titleText, Normalizer.Form.NFD) );

    		String cacheText = teaser.getCachedText();
    		if ( null != cacheText) teaser.setCacheText(
    			Normalizer.normalize(cacheText, Normalizer.Form.NFD) );
    	}
    	
		return true;
		
	}
	
}
