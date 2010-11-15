package com.bizosys.hsearch.query;

import com.bizosys.oneline.SystemFault;

public class DocWeight {

	/**
	 * Document ID
	 */
	String id;
	
	/**
	 * Document weight
	 */
	float wt;
	
	/**
	 * Constructor
	 * @param id
	 * @param wt
	 */
	public DocWeight(String id, float wt) {
		this.id = id;
		this.wt = wt;
	}
	
	public void add(float addition) {
		this.wt = this.wt + addition;
	}
	
	public int compare(DocWeight a)  {
		if ( this.wt > a.wt) return -1;
		else if ( this.wt < a.wt) return 1;
		else return 0;
	}
	
	static DocWeight temp = null;

	/**
	 * Sort based on the weight in descentding order
	 * @param out
	 * @throws Exception
	 */
	public static void  sort(Object[] out) throws SystemFault {
		try {
			sort ( out, 0, out.length -1 );
		} catch (Exception ex) {
			throw new SystemFault(ex);
		}
	}
	
	/**
	 * Quicksort data elements based on weight
	 * @param idWtL 
	 * @param low0
	 * @param high0
	 * @throws Exception
	 */
	private static void  sort(Object idWtL[], 
			int low0, int high0) throws Exception {
		
    	int low = low0; int high = high0;
    	if (low >= high) return;
    	
        if( low == high - 1 ) {
            if (1 == ((DocWeight)idWtL[low]).compare( (DocWeight)idWtL[high] ) ) {
            	temp = (DocWeight)idWtL[low]; idWtL[low] = idWtL[high]; idWtL[high] = temp;
            }
            return;
    	}

    	DocWeight pivot =(DocWeight) idWtL[(low + high) / 2];
        idWtL[(low + high) / 2] = idWtL[high];
        idWtL[high] = pivot;

        while( low < high ) {
            while ( ((DocWeight)idWtL[low]).compare( (DocWeight)pivot )  != 1  && low < high) low++;
            while (pivot.compare((DocWeight)idWtL[high]) != 1 && low < high ) high--;
            if( low < high ) {
                temp = (DocWeight)idWtL[low]; idWtL[low] = idWtL[high]; idWtL[high] = temp;
            }
        }

        idWtL[high0] = idWtL[high]; idWtL[high] = pivot;
    	sort(idWtL, low0, low-1);
    	sort(idWtL, high+1, high0);
	}
	
}
