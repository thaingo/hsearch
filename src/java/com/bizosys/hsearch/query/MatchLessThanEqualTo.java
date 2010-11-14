package com.bizosys.hsearch.query;

import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.common.StorableList;

public class MatchLessThanEqualTo implements IMatch {

	private static MatchLessThanEqualTo instance = null; 
	public static IMatch getInstance() {
		if ( null != instance) return instance;
		instance = new MatchLessThanEqualTo();
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
