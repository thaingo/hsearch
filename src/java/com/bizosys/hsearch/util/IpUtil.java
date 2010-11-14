package com.bizosys.hsearch.util;

import com.bizosys.oneline.util.StringUtils;

public class IpUtil {
	public static final int computeHouse(String strIp) {
		int ipHashed = 0;
		String[] ipAddrDivided = StringUtils.getStrings(strIp, ".");
		if ( ipAddrDivided.length == 4) {
			int a = new Integer( ipAddrDivided[0] );
			int b = new Integer( ipAddrDivided[1] );
			int c = new Integer( ipAddrDivided[2] );
			int d = new Integer( ipAddrDivided[3] );
			ipHashed = a * 16777216 + b * 65536  + c * 256 + d;
		}
		return ipHashed;
	}

}
