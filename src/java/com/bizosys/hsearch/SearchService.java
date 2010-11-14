package com.bizosys.hsearch;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;

import org.apache.log4j.Logger;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;
import com.bizosys.oneline.services.Request;
import com.bizosys.oneline.services.Response;
import com.bizosys.oneline.services.Service;
import com.bizosys.oneline.services.ServiceMetaData;
import com.bizosys.oneline.util.StringUtils;

import com.bizosys.hsearch.common.HDocument;
import com.bizosys.hsearch.index.IndexWriter;
import com.bizosys.hsearch.index.RunPlanManager;
import com.bizosys.hsearch.schema.SchemaManager;

public class SearchService implements Service {

	public static Logger l = Logger.getLogger(SearchService.class.getName());
	
	Configuration conf = null;
	int mergeFactor = 10000;
	
	public boolean init(Configuration conf, ServiceMetaData meta) {
		this.conf = conf;
		this.mergeFactor = conf.getInt("merge.factor", 10000);
		try {
			l.info("Initializing Search Scheme.");
			SchemaManager.getInstance().init(conf, meta);
			l.info("Search Scheme initialized.");
			return true;
		} catch (Exception ex) {
			l.fatal("Search Service Initialization Failed.");
			return false;
		}
		
	}

	public void stop() {
	}
	
	public String getName() {
		return "SearchService";
	}	
	
	public void process(Request req, Response res) {
		
		String action = req.action; 

		try {

			if ( "index".equals(action) ) {
				this.doIndex(req, res);

			}  else if ( "index.ids".equals(action) ) {
				this.doIndex(req, res);
				
			}  else if ( "look.dictionary".equals(action) ) {
				this.matchDictionary(req,res);
				
			} else if ( "search".equals(action) ) {
				this.doSearch(req, res);

			} else if ( "get".equals(action) ) {
				this.doGet(req, res);
				
			} else if ( "update".equals(action) ) {
				doUpdate(req, res);
				
			} else if ( "delete".equals(action) ) {
				doDelete(req, res);
				
			} else {
				res.error("Failed Unknown operation : " + action);
			}
		} catch (Exception ix) {
			l.fatal("SearchService > ", ix);
			res.error("Failure : SearchService:" + action + " " + ix.getMessage());
		}
	}

	/**
	 * Indexes a String. For indexing along with File Upload
	 * happens through fileuploadservlet.xml
	 * @param req
	 * @param res
	 * @throws ApplicationError
	 * @throws ApplicationFault
	 * @throws IOException
	 * @throws ParseException
	 */
	private void doIndex(Request req, Response res) 
	throws SystemFault, ApplicationFault{

		HDocument hdoc = (HDocument) req.getObject("hdoc", true);
		String runPlan = req.getString("runplan", true,true,true);
		
		if ( StringUtils.isEmpty(runPlan) ) {
			res.error("Runplan is missing");
			return;
		}
		
		List<PipeIn> pipes = 
			RunPlanManager.getInstance().compilePlan(StringUtils.getStrings(runPlan));
		new IndexWriter().insert(hdoc, pipes, mergeFactor);
		res.writeXmlString("OK");
		
		
	}
	
	/**
	 * Deletes a document for the given Id.
	 * @param req
	 * @param res
	 * @throws ApplicationFault
	 */
	private void doDelete(Request req, Response res) throws IOException, ParseException {
	}

	/**
	 * Update a specific Field
	 * @param req
	 * @throws Exception
	 * @throws ApplicationFault
	 * @throws ParseException
	 * @throws ApplicationError
	 */
	private void doUpdate(Request req, Response res) throws IOException, ParseException {
	}

	/**
	 * Searches for a query
	 * @param req
	 * @param res
	 * @throws ApplicationError
	 * @throws ApplicationFault
	 * @throws IOException
	 */
	private void doSearch(Request req, Response res) throws IOException, ParseException {
	}
	
	/**
	 * Gets a codument
	 * @param req
	 * @param res
	 * @throws ApplicationError
	 * @throws ApplicationFault
	 * @throws IOException
	 */
	private void doGet(Request req, Response res) throws IOException, ParseException {
	}

	private void matchDictionary(Request req, Response res) throws IOException, ParseException {
	}
}
