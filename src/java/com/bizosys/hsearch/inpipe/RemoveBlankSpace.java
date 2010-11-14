package com.bizosys.hsearch.inpipe;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.oneline.ApplicationFault;
import org.apache.oneline.SystemFault;
import org.apache.oneline.conf.Configuration;
import org.apache.oneline.pipes.PipeIn;

import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.DocTeaser;

public class RemoveBlankSpace implements PipeIn {

	Pattern pattern = null;
	String replaceStr = " ";
	
	public boolean commit() throws ApplicationFault, SystemFault {
		return true;
	}
	
	

	public PipeIn getInstance() {
		return this;
	}

	public String getName() {
		return "RemoveCachedText";
	}

	public boolean init(Configuration conf) throws ApplicationFault, SystemFault {
		this.pattern = Pattern.compile("\\s+");		
		return true;
	}

	public boolean visit(Object docObj) throws ApplicationFault, SystemFault {
		if ( null == docObj) return false;
		Doc doc = (Doc) docObj;
		
    	DocTeaser teaser = doc.teaser;
    	if ( null != teaser) {
    		String titleText = teaser.getTitle();
    		if ( null != titleText) teaser.setTitle(strip(titleText));

    		String cacheText = teaser.getCachedText();
    		if ( null != cacheText) teaser.setCacheText(strip(cacheText));
    	}
    	
		return true;
	}

	public synchronized String strip ( String text) {
		Matcher matcher = pattern.matcher(text);
		return matcher.replaceAll(replaceStr);
	}	
	
}
