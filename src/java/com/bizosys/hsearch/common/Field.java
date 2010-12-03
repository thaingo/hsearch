package com.bizosys.hsearch.common;

import com.bizosys.oneline.ApplicationFault;

public interface Field {
	boolean isIndexable();
	boolean isAnalyze();
	boolean isStore();
	ByteField getByteField() throws ApplicationFault;
}
