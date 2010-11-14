package com.bizosys.hsearch.query;

import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.common.StorableList;

public class MatchWithIn implements IMatch {

	private static MatchWithIn instance = null; 
	public static IMatch getInstance() {
		if ( null != instance) return instance;
		instance = new MatchWithIn();
		return instance;
	}
	public boolean match (Storable termValue, Storable termType, 
			Storable foundValue, Storable foundType) {
		return true;
		
	}
	public boolean match (Storable termValue, StorableList termType, 
			Storable foundValue, Storable foundType){

		return true;
		
	}

}
