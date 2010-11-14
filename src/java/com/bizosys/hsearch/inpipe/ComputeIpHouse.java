package com.bizosys.hsearch.inpipe;

import org.apache.oneline.ApplicationFault;
import org.apache.oneline.SystemFault;
import org.apache.oneline.conf.Configuration;
import org.apache.oneline.pipes.PipeIn;

import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.DocMeta;
import com.bizosys.hsearch.util.IpUtil;

public class ComputeIpHouse implements PipeIn {

	public boolean commit() throws ApplicationFault, SystemFault {
		return true;
	}

	public PipeIn getInstance() {
		return this;
	}

	public String getName() {
		return "ComputeIpHouse";
	}

	public boolean init(Configuration conf) throws ApplicationFault, SystemFault {
		return true;
	}

	public boolean visit(Object docObj) throws ApplicationFault, SystemFault {
		if ( null == docObj) return false;
		Doc doc = (Doc) docObj;
		
    	DocMeta meta = doc.meta;
    	if ( null == meta ) return false;
    	meta.ipHouse = IpUtil.computeHouse(doc.ipAddress);
		return true;
	}
	
}
