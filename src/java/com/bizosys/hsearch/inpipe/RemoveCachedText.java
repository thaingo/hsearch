package com.bizosys.hsearch.inpipe;

import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;

import com.bizosys.hsearch.index.Doc;

public class RemoveCachedText implements PipeIn {

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
		return true;
	}

	public boolean visit(Object docObj) throws ApplicationFault, SystemFault {
		if ( null == docObj) return false;
		Doc doc = (Doc) docObj;
		if ( null != doc.teaser) doc.teaser.cacheText = null;
		return true;
		
	}
	
}
