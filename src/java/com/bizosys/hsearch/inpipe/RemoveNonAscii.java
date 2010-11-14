package com.bizosys.hsearch.inpipe;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.oneline.ApplicationFault;
import org.apache.oneline.SystemFault;
import org.apache.oneline.conf.Configuration;
import org.apache.oneline.pipes.PipeIn;

import com.bizosys.hsearch.index.Doc;

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
