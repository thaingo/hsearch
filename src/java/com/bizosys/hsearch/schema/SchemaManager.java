package com.bizosys.hsearch.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.services.Request;
import com.bizosys.oneline.services.Response;
import com.bizosys.oneline.services.Service;
import com.bizosys.oneline.services.ServiceMetaData;

import com.bizosys.hsearch.hbase.HWriter;

/**
 * This schema creates in pristine mode 154 directories and 378 files.
 * @author karan
 *
 */
public class SchemaManager  implements Service {
	
	private static SchemaManager instance = null;
	
	public static final SchemaManager getInstance() {
		if ( null != instance) return instance;
		synchronized (SchemaManager.class) {
			if ( null != instance) return instance;
			instance = new SchemaManager();
		}
		return instance;
	}
	
	public SchemaManager(){
		instance = this;
	}
	
	private static final String NO_COMPRESSION = Compression.Algorithm.NONE.getName();

	private String searchCompression = NO_COMPRESSION;	
	private boolean searchBlockCache = true;	
	private int searchBlockSize = HColumnDescriptor.DEFAULT_BLOCKSIZE;	
	private String searchBloomFilter = StoreFile.BloomType.ROWCOL.toString();;	
	private int searchRepMode = HConstants.REPLICATION_SCOPE_GLOBAL;;	

	private String teaserCompression = NO_COMPRESSION;	
	private boolean teaserBlockCache = true;	
	private int teaserBlockSize = HColumnDescriptor.DEFAULT_BLOCKSIZE;	
	private String teaserBloomFilter = StoreFile.BloomType.NONE.toString();;	
	private int teaserRepMode = HConstants.REPLICATION_SCOPE_GLOBAL;;	

	private String contentCompression = Compression.Algorithm.GZ.getName();	
	private boolean contentBlockCache = false;	
	private int contentBlockSize = HColumnDescriptor.DEFAULT_BLOCKSIZE;	
	private String contentBloomFilter = StoreFile.BloomType.NONE.toString();;	
	private int contentRepMode = HConstants.REPLICATION_SCOPE_GLOBAL;;	

	private String idMapCompression = NO_COMPRESSION;	
	private boolean idMapBlockCache = false;	
	private int idMapBlockSize = HColumnDescriptor.DEFAULT_BLOCKSIZE;	
	private String idMapBloomFilter = StoreFile.BloomType.NONE.toString();	
	private int idMapRepMode = HConstants.REPLICATION_SCOPE_GLOBAL;	

	private String invertCompression = NO_COMPRESSION;	
	private boolean invertBlockCache = false;	
	private int invertBlockSize = HColumnDescriptor.DEFAULT_BLOCKSIZE;	
	private String invertBloomFilter = StoreFile.BloomType.NONE.toString();	
	private int invertRepMode = HConstants.REPLICATION_SCOPE_GLOBAL;	

	public boolean init(Configuration conf, ServiceMetaData meta) {
		try {
			createPreview(conf);
			createContent(conf);
			createIdMap(conf);
			createInvert(conf);
			createConfigs(conf);
			createDictionary(conf);
			return true;
			
		} catch (Exception sf) {
			HLog.l.fatal(sf);
			return false;
		} 
	}
	
	/**
	 * Column Family : Search (META, SOCIAL, BUCKET)
	 * 				 : Teaser (ID, URL, TITLE, CACHE, PREVIEW)
  	 */
	public void createPreview(Configuration conf) throws SystemFault, ApplicationFault{
		
		int rev = conf.getInt("record.revision",1);
		List<HColumnDescriptor> colFamilies = new ArrayList<HColumnDescriptor>();
		
		HColumnDescriptor search = 
			new HColumnDescriptor( IOConstants.SEARCH_BYTES,
				rev, searchCompression, 
				false, searchBlockCache,
				searchBlockSize,					
				HConstants.FOREVER, 
				searchBloomFilter,
				searchRepMode);
		
		HColumnDescriptor teaser = 
			new HColumnDescriptor( IOConstants.TEASER_BYTES,
				1, teaserCompression, 
				false, teaserBlockCache,
				teaserBlockSize,					
				HConstants.FOREVER, 
				teaserBloomFilter,
				teaserRepMode);

		colFamilies.add(search);
		colFamilies.add(teaser);
		HWriter.create(IOConstants.TABLE_PREVIEW, colFamilies);
		
	}
		
	
	/**
	 * Column Family : Body (FIELDS)
	 * 				 : CITATION ( CITATION_FROM, CITATION_TO ) 
  	 */
	public void createContent(Configuration conf) throws SystemFault, ApplicationFault{
		
		int rev = conf.getInt("record.revision",1);
		List<HColumnDescriptor> colFamilies = new ArrayList<HColumnDescriptor>();
		
		HColumnDescriptor fields = 
			new HColumnDescriptor( IOConstants.CONTENT_FIELDS_BYTES,
				1, contentCompression, 
				false, contentBlockCache,
				contentBlockSize,					
				HConstants.FOREVER, 
				contentBloomFilter,
				contentRepMode);
		
		HColumnDescriptor citation = 
			new HColumnDescriptor( IOConstants.CONTENT_CITATION_BYTES,
				1, contentCompression, 
				false, contentBlockCache,
				contentBlockSize,					
				HConstants.FOREVER, 
				contentBloomFilter,
				contentRepMode);

		colFamilies.add(fields);
		colFamilies.add(citation);
		HWriter.create(IOConstants.TABLE_CONTENT, colFamilies);
		
	}
	
	/**
	 * emp123(ori document id) = b1 (bucket id), 2343(Bucket doc Serial No)
	 * The mapped document id =   b1_2343 (This is Unique ID)
	 * @throws SystemFault
	 * @throws ApplicationFault
	 */
	private void createIdMap(Configuration conf) throws SystemFault, ApplicationFault{
		
		HColumnDescriptor mapping = 
			new HColumnDescriptor( IOConstants.NAME_VALUE_BYTES,
				1, idMapCompression, 
				false, idMapBlockCache,
				idMapBlockSize,					
				HConstants.FOREVER, 
				idMapBloomFilter,
				idMapRepMode);
		
		List<HColumnDescriptor> colFamilies = new ArrayList<HColumnDescriptor>();
		colFamilies.add(mapping);
		HWriter.create(IOConstants.TABLE_IDMAP, colFamilies);
		
	}
	
	/**
	 * ABINASH
	 * Table “A = First Character Map”, 
	 * 	  Column Family 7 = Term Length, 
	 * 		  Column H = Last Character
	 * ID = Bucket Id
	 *   keyword_hash, 
	 *   keyword
	 *   total docs(#)
	 *   [doc type 1, doc type 2, …, doc type n] *
	 *   [term type 1, term type 2, …, term type n] *
	 *   [term weight 1, term weight 2, …, term weight n] *
	 *   [term first pos 1, term first pos 2, …, term pos n]*
	 *   [Bucket doc Serial ID 1, Bucket doc Serial ID 2, …, n]*
	 */
	private void createInvert(Configuration conf) throws SystemFault, ApplicationFault{
		List<HColumnDescriptor> colFamilies = new ArrayList<HColumnDescriptor>();
		
		for ( char t : ILanguageMap.ALL_COLS) {
			
			colFamilies.clear();
			
			for (char c : ILanguageMap.ALL_FAMS) {
				HColumnDescriptor indexCol = 
					new HColumnDescriptor( new byte[] {(byte) c},
						1, invertCompression, 
						false, invertBlockCache,
						invertBlockSize,					
						HConstants.FOREVER, 
						invertBloomFilter,
						invertRepMode);
				colFamilies.add(indexCol);
			}
			
			HWriter.create(new String( new char[]{t}), colFamilies);
		}
		
	}
	
	public void createConfigs(Configuration conf)throws SystemFault, ApplicationFault{
		HColumnDescriptor config = 
			new HColumnDescriptor( IOConstants.NAME_VALUE_BYTES,
				1, NO_COMPRESSION, 
				false, true,
				HColumnDescriptor.DEFAULT_BLOCKSIZE,					
				HConstants.FOREVER, 
				StoreFile.BloomType.NONE.toString(),
				HConstants.REPLICATION_SCOPE_GLOBAL);
		
		List<HColumnDescriptor> colFamilies = new ArrayList<HColumnDescriptor>();
		colFamilies.add(config);
		HWriter.create(IOConstants.TABLE_CONFIG, colFamilies);
	}
	
	public void createDictionary(Configuration conf)throws SystemFault, ApplicationFault{
		HColumnDescriptor dict = 
			new HColumnDescriptor( IOConstants.DICTIONARY_BYTES,
				1, NO_COMPRESSION, 
				false, true,
				HColumnDescriptor.DEFAULT_BLOCKSIZE,					
				HConstants.FOREVER, 
				StoreFile.BloomType.NONE.toString(),
				HConstants.REPLICATION_SCOPE_GLOBAL);
		
		List<HColumnDescriptor> colFamilies = new ArrayList<HColumnDescriptor>();
		colFamilies.add(dict);
		HWriter.create(IOConstants.TABLE_DICTIONARY, colFamilies);
	}
	
	
	public ILanguageMap getLanguageMap(Locale l) throws ApplicationFault {
		if ( Locale.ENGLISH.getDisplayLanguage().equals(
			l.getDisplayLanguage()) ) return new EnglishMap();
		throw new ApplicationFault(l.toString() + " is not supported yet.");
	}
	
	public String getName() {
		return "Schema";
	}

	public void process(Request arg0, Response arg1) {
	}

	public void stop() {
	}
	
	public static void main(String[] args) throws Exception {
		new SchemaManager().init(new Configuration(), null);
	}
	
}