package com.bizosys.hsearch.index;

import java.util.List;

import com.bizosys.oneline.ApplicationFault;

import com.bizosys.hsearch.hbase.NV;

public interface IDimension {
	
	void toNVs(List<NV> nvs) throws ApplicationFault;
	void cleanup();
}
