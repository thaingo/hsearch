package com.bizosys.hsearch.outpipe;

import java.util.ArrayList;
import java.util.List;

import com.bizosys.hsearch.util.FileReaderUtil;
import com.bizosys.oneline.util.StringUtils;

public class BuildTeaser_Highlighter {
	private static final byte[] WORD_DELIMITERS = new String(" .,;\r\n").getBytes();
	private static final int WORD_DELIMITERS_LENGTH = WORD_DELIMITERS.length;
	
	private byte[] bContent;
	private byte[][] bWords;
	private int csize;
	private int wsize[];
	
	public BuildTeaser_Highlighter(byte[] content, String[] words) {
		this.bContent = content;
		this.csize = this.bContent.length;
		this.bWords = new byte[words.length][];
		this.wsize = new int[this.bWords.length];
		int loop = 0;
		for (String word : words) 
		{
			this.bWords[loop] = word.getBytes();
			this.wsize[loop] = this.bWords[loop].length;
			System.out.println("Word:" + word + " Length:" + this.wsize[loop]);
			loop++;
		}
		System.out.println("To find in :" + new String(this.bContent));
		
		for (byte delim : WORD_DELIMITERS)
		{
			System.out.println("Delimiter:" + delim);
		}
			
	}
	
	public List<WordPosition> findTerms() {
		int wordCount = this.wsize.length;
		byte cbyte;
		
		List<WordPosition> posL = new ArrayList<WordPosition>();
		
		for (int ci = 0; ci < this.csize; ci++) {
			cbyte = this.bContent[ci];
			int cj = 0;
			for (; cj < WORD_DELIMITERS_LENGTH; cj++) {
				if (cbyte == WORD_DELIMITERS[cj]) break; 
			}
			
			if (cj < WORD_DELIMITERS_LENGTH) continue; //Got a word delimiter
			
			int wi = 0;
			for (wi = 0; wi < wordCount; wi++) {
				if (cbyte != this.bWords[wi][0]) continue; //First character matched with one word. Possible 2 words with same first char 
				int wj = 1;
				for (; wj < this.wsize[wi]; wj++) {
					/**
					 * Try matching the word. This can be speeded up by some 
					 * tricks like checking the last char first, middle and so on
					 * instead of sequentially.
					 */
					if (this.bWords[wi][wj] != this.bContent[ci + wj]) break;   
				}
				
				/**
				 * I wish there is a goto statement in java to 
				 * just go where we want instead of break and if
				 */
				if (wj < this.wsize[wi]) continue;
				
				/**
				 *The word has matched. Check for the next char to
				 *be a word delimiter from WORD_DELIMITERS  
				 */
				cbyte = this.bContent[ci + this.wsize[wi]]; 
				for (wj = 0; wj < WORD_DELIMITERS_LENGTH; wj++) {
					if (cbyte == WORD_DELIMITERS[wj]) break; 
				}
				
				//	Go to the next word
				if (wj >= WORD_DELIMITERS_LENGTH) continue;  
				
				//Found the word, so add the position
				posL.add(new WordPosition(wi, ci, (ci + this.wsize[wi])));
				
				/**
				 * Move the reader till the end of the word and into the space.
				 * The for loop will advance it to the next
				 */
				ci = ci + this.wsize[wi]; 
			}
			
			//	Found a word, just go back to the main loop
			if (wi < wordCount) continue; 
			
			//Skip to the start of the next word
			for (; ci < this.csize; ci++) {
				cbyte = this.bContent[ci];
				for (cj = 0; cj < WORD_DELIMITERS_LENGTH; cj++) {
					if (cbyte == WORD_DELIMITERS[cj]) break; 
				}
				//	Got a word delimiter
				if (cj < WORD_DELIMITERS_LENGTH) break; 
			}
			
		}
		return posL;
	}
	
	public static void main(String[] args) throws Exception
	{
		String[] wordL = new String[]{"abinash", "karan", "hbase"};
		byte[] content = new String("I know abinash karan is a wonderful person. He worked on hsearch, an open source search engine built on hbase. " +
				"This is a wonderful work by abinash, so lets enjoy.").getBytes();
		
		long start = System.currentTimeMillis();
		List<WordPosition> posL = new BuildTeaser_Highlighter(content, wordL).findTerms();
		System.out.println("Time taken in ms:" + (System.currentTimeMillis() - start) );
		System.out.println(posL);
	}
	
	public static void main1(String[] args) throws Exception
	{
		if (args == null || args.length != 2) 
		{
			System.out.println("Expecting 2 arguments - filename and search words like file.txt abinash_karan_hbase");
			return;
		}
		
		String fileName = args[0];
		String words = args[1];
		String[] wordL = StringUtils.getStrings(words, "_");
		byte[] content = FileReaderUtil.getBytes(FileReaderUtil.getFile(fileName));
		
		List<WordPosition> posL = new BuildTeaser_Highlighter(content, wordL).findTerms();
		System.out.println(posL);
	}
	
	public static class WordPosition
	{
		int index; //Query keyword position (abinash karan hbase = 0,1,2
		int start; //start position of the word in the given corpus
		int end;   //End position start position + word length
		
		public WordPosition(int index, int start, int end) {
			this.index = index;
			this.start = start;
			this.end = end;
		}
		
		public String toString() {
			return "index:" + index + ", start:" + start + ", end:" + end;
		}
	}
}
