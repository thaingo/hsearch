package com.bizosys.hsearch.index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.oneline.ApplicationFault;
import org.apache.oneline.conf.Configuration;
import org.apache.oneline.pipes.PipeIn;
import org.apache.oneline.services.Request;
import org.apache.oneline.services.Response;
import org.apache.oneline.services.Service;
import org.apache.oneline.services.ServiceMetaData;
import org.apache.oneline.util.StringUtils;

import com.bizosys.hsearch.inpipe.L;

public class RunPlanManager implements Service {
	
	private static RunPlanManager instance = null;
	
	public static RunPlanManager getInstance() {
		if ( null != instance) return instance;
		synchronized (RunPlanManager.class) {
			if ( null != instance) return instance;
			instance = new RunPlanManager();
		}
		return instance;
	}
	
	Map<String, PipeIn> pipes = new HashMap<String, PipeIn>(); 
	
	public RunPlanManager(){
	}
	
	public boolean init(Configuration conf, ServiceMetaData meta) {
		String pipes = conf.get("inpipes", "");
		String[] pipeL = StringUtils.getStrings(pipes);
		if ( null == pipeL) return true;
		try {
			for (String pipe : pipeL) {
				L.l.info("Initializing Pipe :" + pipe);
				PipeIn inpipe = (PipeIn) Class.forName(pipe).newInstance();
				if ( ! inpipe.init(conf) ) {
					L.l.fatal("Pipe Initialization Failure :" + pipe);
					return false;
				}
				this.pipes.put(inpipe.getName(), inpipe);
			}
			return true;
		} catch (Exception e) {
			L.l.fatal("Pipe Initialization Failure :" , e );
			return false;
		}		
	}

	public void process(Request req, Response res) {
		
	}

	public void stop() {
		this.pipes.clear();
	}	
	
	/**
	 * Gives all the available steps for processing
	 * @return
	 */
	public Set<String> getAvailableSteps() {
		return pipes.keySet();
	}
	
	/**
	 * Resolves the plan for a given step names.
	 * TODO:// Sanity check for the intelligencec of step sequencing..
	 * @param stepNames
	 * @return
	 * @throws ApplicationFault
	 */
	public List<PipeIn> compilePlan(String[] stepNames) throws ApplicationFault {
		
		List<PipeIn> anvils = new ArrayList<PipeIn>(stepNames.length);
		for (String step : stepNames) {
			PipeIn aPipe = pipes.get(step);
			if ( null == aPipe) throw new ApplicationFault("Pipe Not Found: " + step);
			anvils.add(aPipe.getInstance());
		}
		return anvils;
	}
	
	public List<PipeIn> compilePlan(List<String> stepNames) throws ApplicationFault {
		
		List<PipeIn> anvils = new ArrayList<PipeIn>(stepNames.size());
		for (String step : stepNames) {
			PipeIn aPipe = pipes.get(step);
			if ( null == aPipe) throw new ApplicationFault("Pipe Not Found: " + step);
			anvils.add(aPipe);
		}
		return anvils;
	}

	public String getName() {
		return "RunPlanManager";
	}
	
}
