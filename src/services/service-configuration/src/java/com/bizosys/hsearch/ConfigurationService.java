package com.bizosys.hsearch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.bizosys.hsearch.index.DocumentType;
import com.bizosys.hsearch.index.TermType;
import com.bizosys.hsearch.inpipe.util.StopwordManager;
import com.bizosys.hsearch.inpipe.util.StopwordRefresh;
import com.bizosys.hsearch.util.FileReaderUtil;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.services.Request;
import com.bizosys.oneline.services.Response;
import com.bizosys.oneline.services.Service;
import com.bizosys.oneline.services.ServiceMetaData;
import com.bizosys.oneline.util.StringUtils;

public class ConfigurationService implements Service {
	public static Logger l = Logger.getLogger(ConfigurationService.class.getName());
	
	Configuration conf = null;
	
	public boolean init(Configuration conf, ServiceMetaData meta) {
		this.conf = conf;
		return true;
	}

	public void stop() {
	}
	
	public String getName() {
		return "ConfigurationService";
	}	
	
	public void process(Request req, Response res) {
		
		String action = req.action; 

		try {

			if ( "document.typecodes".equals(action) ) {
				this.addDocumentType(req, res);

			}  else if ( "term.typecodes".equals(action) ) {
				this.addTermType(req, res);
				
			}  else if ( "stopwords.add".equals(action) ) {
				this.addStopwords(req, res);
				
			} else {
				res.error("Failed Unknown operation : " + action);
			}
		} catch (Exception ix) {
			l.fatal("ConfigurationService > ", ix);
			res.error("Failure : ConfigurationService:" + action + " " + ix.getMessage());
		}
	}
	
	@SuppressWarnings("unchecked")
	private void addDocumentType(Request req, Response res) throws ApplicationFault, SystemFault{
		Map<String,Byte> codes = (Map<String,Byte>) req.getObject("typecodes", true);
		DocumentType type = DocumentType.getInstance();
		for (String key : codes.keySet()) {
			type.types.put(key, codes.get(key)); 
		}
		try {
			type.persist();
		} catch (IOException ex) {
			throw new SystemFault(ex);
		}
		res.writeXmlString("OK");
	}
	
	@SuppressWarnings("unchecked")
	private void addTermType(Request req, Response res) throws ApplicationFault, SystemFault{
		Map<String,Byte> codes = (Map<String,Byte>) req.getObject("typecodes", true);
		TermType type = TermType.getInstance();
		for (String key : codes.keySet()) {
			type.types.put(key, codes.get(key)); 
		}
		try {
			type.persist();
		} catch (IOException ex) {
			throw new SystemFault(ex);
		}
		res.writeXmlString("OK");
	}
	
	private void addStopwords(Request req, Response res) throws ApplicationFault, SystemFault{
		String stopwords = req.getString("stopwords", true, true, true);
		List<String> stopwordL = StringUtils.fastSplit(stopwords, ',');
		StopwordManager.getInstance().setStopwords(stopwordL);
		new StopwordRefresh().process(); //A 30mins job is done in sync mode
		res.writeXmlString("OK");
	}
	
	
}
