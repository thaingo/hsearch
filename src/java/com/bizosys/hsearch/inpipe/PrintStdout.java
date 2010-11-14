package com.bizosys.hsearch.inpipe;

import org.apache.oneline.ApplicationFault;
import org.apache.oneline.SystemFault;
import org.apache.oneline.conf.Configuration;
import org.apache.oneline.pipes.PipeIn;

import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.DocTeaser;
import com.bizosys.hsearch.index.DocTerms;
import com.bizosys.hsearch.index.Term;

public class PrintStdout implements PipeIn {

	public boolean commit() throws ApplicationFault, SystemFault {
		return true;
	}

	public PipeIn getInstance() {
		return this;
	}

	public String getName() {
		return "PrintStdout";
	}

	public boolean init(Configuration conf) throws ApplicationFault, SystemFault {
		return true;
	}

	public boolean visit(Object docObj) throws ApplicationFault, SystemFault {
		if ( null == docObj) return false;
		Doc doc = (Doc) docObj;
		
    	DocTeaser teaser = doc.teaser;
    	if ( null != teaser) {
    		System.out.println("Url:" + teaser.getUrl() );
    		System.out.println("Title :" + teaser.getTitle() );
    		System.out.println("Preview :" + teaser.getPreview() );
    		System.out.println("Cache :" + teaser.getCachedText() );
    	}
    	DocTerms terms = doc.terms;
    	StringBuilder sb = new StringBuilder();
    	if ( null != terms) {
    		if ( null != terms.all) {
    			for (Term term: terms.all) {
    				sb.append("Term [").append(term.toString()).append("]\n");
				}
    		}
    	}
    	System.out.println("Terms :" + sb.toString() );

    	return true;
	}
	
}
