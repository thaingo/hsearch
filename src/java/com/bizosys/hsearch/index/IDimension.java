package com.bizosys.hsearch.index;

import java.util.List;

import org.apache.oneline.ApplicationFault;

import com.bizosys.hsearch.hbase.NV;

public interface IDimension {
	
	void toNVs(List<NV> nvs) throws ApplicationFault;
	void cleanup();
}
