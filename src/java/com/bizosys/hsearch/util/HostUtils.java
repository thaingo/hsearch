package com.bizosys.hsearch.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.bizosys.oneline.util.StringUtils;

public class HostUtils {
	private static String HOST_NAME = null;
	private static String IP = null;
	
	public static final String getHostName() {
		if ( null != HOST_NAME) return HOST_NAME;
		try {
			InetAddress addr = InetAddress.getLocalHost();
			HOST_NAME = addr.getHostName();
		} catch (UnknownHostException ex) {
			HOST_NAME = "localhost";
		}
		if ( StringUtils.isEmpty(HOST_NAME)) HOST_NAME = "localhost";
		return HOST_NAME;
	}

	public static String getIp() {
		if ( null != IP) return IP;
		try {
			InetAddress addr = InetAddress.getLocalHost();
			IP = addr.getHostAddress();
		} catch (UnknownHostException ex) {
			IP = "127.0.0.1";
		}
		if ( StringUtils.isEmpty(IP)) IP = "127.0.0.1";
		return IP;
	}

}
