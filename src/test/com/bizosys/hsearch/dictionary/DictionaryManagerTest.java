package com.bizosys.hsearch.dictionary;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import junit.framework.TestCase;

import org.apache.oneline.conf.Configuration;
import org.apache.oneline.util.StringUtils;

import com.bizosys.ferrari.TestFerrari;
import com.bizosys.hsearch.dictionary.DictEntry;
import com.bizosys.hsearch.dictionary.DictionaryManager;

public class DictionaryManagerTest extends TestCase {

	public static void main(String[] args) throws Exception {
		DictionaryManagerTest t = new DictionaryManagerTest();
		DictionaryManager d = new DictionaryManager();
		d.init(new Configuration(), null);
        TestFerrari.testAll(t);
		//t.testWordListing();
	}
	
	public void cleanup() throws Exception {
		DictionaryManager s = DictionaryManager.getInstance();
		s.deleteAll();
	}

	public void testRandomAddEntry(String keyword) throws Exception {
		DictionaryManager s = DictionaryManager.getInstance();
		DictEntry e1 = new DictEntry(keyword,"RANDOM", 1, null,null);
		Hashtable<String, DictEntry> entries = new Hashtable<String, DictEntry>();
		entries.put(e1.fldWord, e1);
		s.add(entries);
	}
	
	private void testRandomAddEntry(String keyword, String type ) throws Exception {
		DictionaryManager s = DictionaryManager.getInstance();
		DictEntry e1 = new DictEntry(keyword,type, 1, null,null);
		Hashtable<String, DictEntry> entries = new Hashtable<String, DictEntry>();
		entries.put(e1.fldWord, e1);
		s.add(entries);
	}
	
	public void testAddEntries(String keyword1, String keyword2, String keyword3, 
			String keyword4, String keyword5, String keyword6, String keyword7,  
			String keyword8, String keyword9, String keyword10) throws Exception {

		DictionaryManager s = DictionaryManager.getInstance();
		String[] keywords = new String[] {
				keyword1, keyword2, keyword3, keyword4, keyword5,
				keyword6, keyword7, keyword8, keyword9, keyword10
		};
		Hashtable<String, DictEntry> hashes = new Hashtable<String, DictEntry>();
		for (String keyword : keywords) {
			DictEntry entry = new DictEntry(keyword);
			hashes.put(keyword, entry);
		}
		s.add(hashes);
		
		for (String keyword : keywords) {
			DictEntry entry = s.get(keyword);
			assertNotNull(entry);
			assertEquals(keyword, entry.fldWord);
		}
	}
	

	public void testGetEntry(String keyword) throws Exception {
		DictionaryManager s = DictionaryManager.getInstance();
		testRandomAddEntry(keyword, "RANDOM");
		DictEntry entry = s.get(keyword);
		assertNotNull(entry);
		assertEquals(keyword, entry.fldWord);
		assertEquals("RANDOM", entry.fldType);
	}
	
	public void testGetEmpty() throws Exception {
		DictionaryManager s = DictionaryManager.getInstance();
		DictEntry entry = s.get("");
		assertNull(entry);
	}
	
	public void testNonExisting() throws Exception {
		DictionaryManager s = DictionaryManager.getInstance();
		DictEntry entry = s.get("__aSDKJ234KSAKL1adsa");
		assertNull(entry);
	}

	public void testSpellCorrection() throws Exception {
		DictionaryManager s = DictionaryManager.getInstance();
		testRandomAddEntry("Abinasha", "Fuzzy");
		s.refresh();
		List<String> fuzzyWords = s.getSpelled("abinash");		
		assertNotNull(fuzzyWords);
		if ( 1 != fuzzyWords.size()) System.out.println(StringUtils.listToString(fuzzyWords, '\n'));
		assertEquals(1 , fuzzyWords.size());
		assertEquals("Abinasha", fuzzyWords.get(0));
	}

	public void testWildcard() throws Exception {
		DictionaryManager s = DictionaryManager.getInstance();
		testRandomAddEntry("Abinasha", "");
		s.refresh();
		List<String> regexWords = s.getWildCard(".binasha");		
		assertNotNull(regexWords);
		//System.out.println(">>>>" + StringUtils.listToString(s.getKeywords(), '\n'));
		assertEquals(1, regexWords.size());
		assertEquals("Abinasha", regexWords.get(0));
	}
	
	public void testResolveTypes(String keyword ) throws Exception {
		DictionaryManager s = DictionaryManager.getInstance();
		List<String> lst = new ArrayList<String>();
		lst.add(keyword);
		s.delete(lst);
		Thread.sleep(100);
		
		testRandomAddEntry(keyword, "TYPE1");
		testRandomAddEntry(keyword, "TYPE2");
		DictEntry entry = s.get(keyword);
		assertNotNull(entry);
		
		List<String> types = entry.getTypes();
		assertNotNull(types);
		if ( 2 != types.size()) System.out.println(
			"INSTR: " + "Keyword:[" + keyword + "] , Types =" + 
			StringUtils.listToString(types, '\n') );
		assertEquals(2, types.size() );
		assertTrue("TYPE1".equals(types.get(0)) || "TYPE1".equals(types.get(1)) );
		assertTrue("TYPE2".equals(types.get(0)) || "TYPE2".equals(types.get(1))) ;
	}
	
	public void testTermFrequency(String keyword) throws Exception {
		DictionaryManager s = DictionaryManager.getInstance();
		List<String> lst = new ArrayList<String>();
		lst.add(keyword);
		s.delete(lst);
		Thread.sleep(10);
		
		testRandomAddEntry(keyword, "FREQ");
		testRandomAddEntry(keyword);
		DictEntry entry = s.get(keyword);
		assertNotNull(entry);
		if ( 2 != entry.fldFreq) {
			System.out.println("FREQ:" + keyword);
		}
		assertEquals(2, entry.fldFreq);
	}
	
	private void testWordListing() throws Exception {
		DictionaryManager s = DictionaryManager.getInstance();
		List<String> allWords = s.getKeywords();
		StringBuilder sb = new StringBuilder();
		sb.append("Page 1: " );
		sb.append(StringUtils.listToString(allWords, '\t'));
		System.out.println(sb.toString());
		sb.delete(0, sb.capacity());
		int page = 2;
		String lastWord = null;
		while ( allWords.size() > 1000) {
			lastWord = allWords.get(allWords.size() - 1);
			allWords.clear();
			allWords = s.getKeywords(lastWord);
			sb.append("Page " ).append(page++).append(" : ");
			sb.append(StringUtils.listToString(allWords, '\t'));
			System.out.println(sb.toString());
			sb.delete(0, sb.capacity());
		}
		
	}

}
