package com.bizosys.hsearch.query;

import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.common.StorableList;

public interface IMatch {
	
	public final static int ENDS_WITH = 1;
	public final static int EQUAL_TO = 2;
	public final static int GREATER_THAN = 3;
	public final static int GREATER_THAN_EQUALTO = 4;
	public final static int LESS_THAN = 5;
	public final static int LESS_THAN_EQUALTO = 6;
	public final static int PATTERN_MATCH = 7;
	public final static int RANGE = 8;
	public final static int STARTS_WITH = 9;
	public final static int WITH_IN = 10;
	
	public boolean match (Storable termValue, Storable termType, 
			Storable foundValue, Storable foundType);
	public boolean match (Storable termValue, StorableList termType,
			Storable foundValue, Storable foundType);
}
