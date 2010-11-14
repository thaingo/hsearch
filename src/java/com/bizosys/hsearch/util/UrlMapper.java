package com.bizosys.hsearch.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.util.StringUtils;

public class UrlMapper {

	public static Logger l = Logger.getLogger(UrlMapper.class.getName());
	private static UrlMapper instance = null;
	
	public static final UrlMapper getInstance()  {
		if ( null != instance) return instance;
		synchronized (UrlMapper.class) {
			if ( null != instance ) return instance;
			instance = new UrlMapper();
			instance.load();
		}
		return instance;
	}
	
	public HashMap<String, String> urls = null;
	public HashMap<String, String> codes = null;
	private final static List<String> EMPTY_ARRAY =  new ArrayList<String>();
	
	public void load() {
		List<String> mappings = null;
		try {
			mappings = FileReaderUtil.toLines("urlmappings");
		} catch (ApplicationFault ex) {
			l.warn("UrlMapper: Could not reading urlmappings file.", ex);
			mappings =  EMPTY_ARRAY;
		}
		urls = new HashMap<String, String>(mappings.size());
		codes = new HashMap<String, String>(mappings.size());
		for (String aMap : mappings) {
			String[] mapVal = StringUtils.getStrings(aMap, '\t');
			urls.put(mapVal[0] , mapVal[1]);
			codes.put(mapVal[1], mapVal[0]);
		}
	}
	
	/**
	 * This encodes to the short form of the URL prefix
	 * @param url
	 * @return
	 */
	public String encoding(String url) {
		
		if ( StringUtils.isEmpty(url)) return null;
		
		
		//Can I get an exact match
		
		if ( urls != null && urls.containsKey(url) ) return urls.get(url);
		
		//Can I get an exact till the last = character.
		int lastEqualto = url.lastIndexOf('=');
		if ( -1 != lastEqualto) {
			lastEqualto = lastEqualto + 1;
			String prefix = url.substring(0,lastEqualto);
			if ( urls.containsKey(prefix) ) 
				return urls.get(prefix) + '~' + url.substring(lastEqualto) ;
		}
		
		//Can I get an exact till the last / character.
		int lastSlash = url.lastIndexOf('?');
		if ( -1 != lastSlash) {
			lastSlash++;
			String prefix = url.substring(0,lastSlash );
			if ( urls.containsKey(prefix) ) 
				return urls.get(prefix) + '~' +url.substring(lastSlash) ;
		}
		
		return url;
	}
	
	/**
	 * This decodes the short form of the URL prefix
	 * @param url
	 * @return
	 */
	public String decoding(String codedUrl) {
		if ( StringUtils.isEmpty(codedUrl)) return null;
		
		//Is thre a direct match
		if ( codes.containsKey(codedUrl) ) return codes.get(codedUrl);
		int division = codedUrl.lastIndexOf('~');
		if ( -1 == division) return codedUrl;
		String code = codedUrl.substring(0,division );
		
		if ( codes.containsKey(code) ) 
			return codes.get(code) + codedUrl.substring(division + 1) ;
		
		return codedUrl;
	}
	
	public static void main(String[] args) throws Exception {
		UrlMapper mapper = UrlMapper.getInstance();
		String equalPrifix = "http://www.bizosys.com/employee.xml/id=23";
		String encoded = mapper.encoding(equalPrifix);
		System.out.println( "Encoding  [" + encoded + "]");
		System.out.println( "Decoding  [" + mapper.decoding(encoded) +"]" );

		System.out.println("\n\n\n");
		equalPrifix = "http://www.bizosys.com/employee?23";
		encoded = mapper.encoding(equalPrifix);
		System.out.println( "Encoding  [" + encoded + "]");
		System.out.println( "Decoding  [" + mapper.decoding(encoded) +"]" );
	
		System.out.println("\n\n\n");
		equalPrifix = "http://www.google.com/employee?23";
		encoded = mapper.encoding(equalPrifix);
		System.out.println( "Encoding  [" + encoded + "]");
		System.out.println( "Decoding  [" + mapper.decoding(encoded) +"]" );

	}
	
}
