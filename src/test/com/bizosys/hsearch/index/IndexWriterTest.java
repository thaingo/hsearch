package com.bizosys.hsearch.index;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.oneline.conf.Configuration;
import org.apache.oneline.pipes.PipeIn;
import org.apache.oneline.services.ServiceFactory;

import com.bizosys.ferrari.TestFerrari;
import com.bizosys.hsearch.common.HDocument;
import com.bizosys.hsearch.common.HField;
import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.index.IndexWriter;
import com.bizosys.hsearch.inpipe.ComputeTokens;
import com.bizosys.hsearch.inpipe.FilterLowercase;
import com.bizosys.hsearch.inpipe.FilterStem;
import com.bizosys.hsearch.inpipe.FilterStopwords;
import com.bizosys.hsearch.inpipe.FilterTermLength;
import com.bizosys.hsearch.inpipe.SaveToDictionary;
import com.bizosys.hsearch.inpipe.SaveToIndex;
import com.bizosys.hsearch.inpipe.TokenizeStandard;
import com.bizosys.hsearch.util.FileReaderUtil;

public class IndexWriterTest extends TestCase {

	IndexWriter s = null;
	
	public static void main(String[] args) throws Exception {
		//NV nv = new NV(IOConstants.NAME_VALUE_BYTES, IOConstants.NAME_VALUE_BYTES);
		//HWriter.truncate(IOConstants.TABLE_CONFIG, nv);

		IndexWriterTest t = new IndexWriterTest();
		Configuration conf = new Configuration();
		ServiceFactory.getInstance().init(conf, null);
		t.s = new IndexWriter();
        TestFerrari.testRandom(t);
	}
	
	public void testIndexTeaser(String id, String title, String teaser) throws Exception {
		
		HDocument hdoc = new HDocument();
		hdoc.originalId = new Storable(id);
		hdoc.title = new Storable(title);
		hdoc.fields = new ArrayList<HField>();
		List<PipeIn> pipes = getStandardPipes();
		s.insert(hdoc, pipes, 20);
	}
	
	public void testIndexDetail(String id, String title, String teaser) throws Exception {
		
		HDocument hdoc = new HDocument();
		hdoc.originalId = new Storable(id);
		hdoc.title = new Storable(title);
		hdoc.fields = new ArrayList<HField>();
		HField fld = new HField("BODY",FileReaderUtil.toString("sample.txt"));
		hdoc.fields.add(fld);
		
		List<PipeIn> pipes = getStandardPipes();
		s.insert(hdoc, pipes, 20);
	}
	
	public void testIndexMerge(String keyword1, String keyword2, String keyword3, 
			String keyword4, String keyword5, String keyword6, String keyword7,  
			String keyword8, String keyword9, String keyword10) throws Exception {
		
		String[] keywords = new String[] {
				keyword1, keyword2, keyword3, keyword4, keyword5,
				keyword6, keyword7, keyword8, keyword9, keyword10
		};

		StringBuilder sb = new StringBuilder();
		List<HDocument> hdocs = new ArrayList<HDocument>(5000); 
		for ( int i=0; i<5000; i++) {
			HDocument hdoc = new HDocument();
			hdoc.originalId = new Storable("ORIG_ID:" + i);
			hdoc.title = new Storable("TITLE:" + i);
			sb.delete(0,sb.capacity());
			for (String k : keywords) {
				sb.append(k).append(i).append(' ');		
			} 
			hdoc.fields = new ArrayList<HField>();
			HField fld = new HField("FLD1",sb.toString());
			hdoc.fields.add(fld);
			hdocs.add(hdoc);
		}
		List<PipeIn> pipes = getStandardPipes();
		s.insert(hdocs, pipes, 10000);
	}

	private List<PipeIn> getStandardPipes() {
		List<PipeIn> pipes = new ArrayList<PipeIn>();
		pipes.add(new TokenizeStandard());
		pipes.add(new FilterStopwords());
		pipes.add(new FilterTermLength());
		pipes.add(new FilterLowercase());
		pipes.add(new FilterStem());
		pipes.add(new ComputeTokens());
		pipes.add(new SaveToIndex());
		pipes.add(new SaveToDictionary());
		//pipes.add(new PrintStdout());
		return pipes;
	}
}
