/*
* Copyright 2010 The Apache Software Foundation
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.bizosys.hsearch.query;

import java.util.List;

import com.bizosys.hsearch.hbase.NVBytes;
import com.bizosys.hsearch.index.DocTeaser;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;

public class DocTeaserWeight extends DocTeaser {
	public float weight;
	
	public DocTeaserWeight(byte[] id, List<NVBytes> inputBytes, float weight) throws ApplicationFault {
		super(id,inputBytes);
		this.weight = weight;
	}
	
	public int compare(DocTeaserWeight a)  {
		if ( this.weight > a.weight) return -1;
		else if ( this.weight < a.weight) return 1;
		else return 0;
	}
	
	public static void  sort(Object[] out) throws SystemFault {
		try {
			sort ( out, 0, out.length -1 );
		} catch (Exception ex) {
			throw new SystemFault(ex);
		}
	}
	
	static DocTeaserWeight temp = null;
	
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
            if (1 == ((DocTeaserWeight)idWtL[low]).compare( (DocTeaserWeight)idWtL[high] ) ) {
            	temp = (DocTeaserWeight)idWtL[low]; idWtL[low] = idWtL[high]; idWtL[high] = temp;
            }
            return;
    	}

    	DocTeaserWeight pivot =(DocTeaserWeight) idWtL[(low + high) / 2];
        idWtL[(low + high) / 2] = idWtL[high];
        idWtL[high] = pivot;

        while( low < high ) {
            while ( ((DocTeaserWeight)idWtL[low]).compare( pivot )  != 1  && low < high) low++;
            while (pivot.compare((DocTeaserWeight)idWtL[high]) != 1 && low < high ) high--;
            if( low < high ) {
                temp = (DocTeaserWeight)idWtL[low]; idWtL[low] = idWtL[high]; idWtL[high] = temp;
            }
        }

        idWtL[high0] = idWtL[high]; idWtL[high] = pivot;
    	sort(idWtL, low0, low-1);
    	sort(idWtL, high+1, high0);
	}		
}
