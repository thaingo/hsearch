package com.bizosys.hsearch.outpipe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BuildTeaserHighlighter {
	private static final byte[] WORD_DELIMITERS = new String(" .,;\r\n").getBytes();
	private static final int WORD_DELIMITERS_LENGTH = WORD_DELIMITERS.length;
	
	private byte[] bContent;
	private byte[][] bWords;
	private int csize;
	private int wsize[];
	
	public BuildTeaserHighlighter() {
	}
	
	public BuildTeaserHighlighter(byte[] content, String[] words) {
		setContent(content);
		setWords(words);
	}

	public void setContent(byte[] content) {
		this.bContent = content;
		this.csize = this.bContent.length;
	}
	
	public void setWords(String[] words) {
		this.bWords = new byte[words.length][];
		this.wsize = new int[this.bWords.length];
		int loop = 0;
		for (String word : words) {
			this.bWords[loop] = word.getBytes();
			this.wsize[loop] = this.bWords[loop].length;
			loop++;
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
	
	public byte[] cutSection (List<WordPosition> wpL, int sectionSize) {
		int start = 0;
		int end = sectionSize;
		int sectionsT = (bContent.length/sectionSize) + 1;
		int[] sections = new int[sectionsT];  
		Arrays.fill(sections, 0);
		
		int cur = 0;
		int max = 0;
		int maxIndex = 0;
		for ( int index=0; index< sectionsT; index++ ) {
			start = index * 180;
			end = start + 180;
			for (WordPosition wp : wpL) {
				if (wp.start > start && wp.end < end) {
					cur = sections[index] + 1;
					sections[index] = cur;
					if ( cur > max ) {
						max = cur;
						maxIndex = index;
					}
				}
			}
		}
		
		int newMaxSection = 0;
		for ( int index=1; index< sectionsT - 1; index++ ) {
			if ( (sections[index] + sections[index+1] ) < max ) {
				start = index * sectionSize + (sectionSize / 2) ;
				end = start + sectionSize;
				int found = 0;
				for (WordPosition wp : wpL) {
					if (wp.start > start && wp.end < end) found++;
				}
				if ( found > max){
					newMaxSection = index;
					max = found; 
				}
			}
		}
		
		if ( newMaxSection > max) max = newMaxSection;
		byte[] sectionB = new byte[sectionSize];
		System.arraycopy(this.bContent, maxIndex++ * sectionSize,
				sectionB , 0, sectionSize);
		return sectionB; 
	}
	

	public static class WordPosition {
		
		public int index; //Query keyword position (abinash karan hbase = 0,1,2
		public int start; //start position of the word in the given corpus
		public int end;   //End position start position + word length
		
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