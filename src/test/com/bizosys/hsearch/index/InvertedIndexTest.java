package com.bizosys.hsearch.index;

import java.util.List;

import junit.framework.TestCase;

public class InvertedIndexTest  extends TestCase {

	public static void main(String[] args) throws Exception {
		InvertedIndexTest t = new InvertedIndexTest();
        //TestFerrari.testAll(t);
		t.testMultipleTermAtMiddle();
	}
	
	public void testSingleTermAtEnd() {
		TermList tl = new TermList();
		Term t1 = new Term("abinash",Term.TERMLOC_BODY,(byte)1,12);
		t1.setDocumentPosition((short)1001);
		t1.setDocumentTypeCode((byte) 44);
		t1.setTermWeight((byte)91);
		
		Term t2 = new Term("abinash",Term.TERMLOC_BODY,(byte)1,22);
		t2.setDocumentTypeCode((byte) 45);
		t2.setTermWeight((byte)92);
		t2.setDocumentPosition((short)1002);
		
		tl.add(t1);
		tl.add(t2);
		
		byte[] deletedB = InvertedIndex.delete(tl.toBytes(), (short) 1002);
		assertNotNull(deletedB);
		List<InvertedIndex> iiL = InvertedIndex.read(deletedB);
		assertNotNull(iiL);
		assertEquals(1, iiL.size());
		assertEquals("abinash".hashCode(), iiL.get(0).hash);
		assertEquals(1, iiL.get(0).docPos.length );
		assertEquals(1, iiL.get(0).dtc.length );
		assertEquals(1, iiL.get(0).ttc.length );
		assertEquals(1, iiL.get(0).tw.length );

		assertEquals((short)1001, iiL.get(0).docPos[0]);
		assertEquals((byte)44, iiL.get(0).dtc[0]);
		assertEquals((byte)1, iiL.get(0).ttc[0]);
		assertEquals((byte)91, iiL.get(0).tw[0]);
		
	}
	
	public void testMultiTermAtEnd() {
		TermList tl = new TermList();
		Term t1 = new Term("abinash",Term.TERMLOC_BODY,(byte)1,12);
		t1.setDocumentPosition((short)1001);
		t1.setDocumentTypeCode((byte) 44);
		t1.setTermWeight((byte)91);
		
		Term t2 = new Term("abinash",Term.TERMLOC_BODY,(byte)1,22);
		t2.setDocumentTypeCode((byte) 45);
		t2.setTermWeight((byte)92);
		t2.setDocumentPosition((short)1002);
		
		Term t3 = new Term("avinash",Term.TERMLOC_BODY,(byte)1,22);
		t3.setDocumentTypeCode((byte) 45);
		t3.setTermWeight((byte)92);
		t3.setDocumentPosition((short)1002);

		tl.add(t1);
		tl.add(t2);
		tl.add(t3);
		byte[] deletedB = InvertedIndex.delete(tl.toBytes(), (short) 1002);
		assertNotNull(deletedB);
		List<InvertedIndex> iiL = InvertedIndex.read(deletedB);
		assertNotNull(iiL);
		assertEquals(1, iiL.size());
		assertEquals("abinash".hashCode(), iiL.get(0).hash);
		assertEquals(1, iiL.get(0).docPos.length );
		assertEquals(1, iiL.get(0).dtc.length );
		assertEquals(1, iiL.get(0).ttc.length );
		assertEquals(1, iiL.get(0).tw.length );

		assertEquals((short)1001, iiL.get(0).docPos[0]);
		assertEquals((byte)44, iiL.get(0).dtc[0]);
		assertEquals((byte)1, iiL.get(0).ttc[0]);
		assertEquals((byte)91, iiL.get(0).tw[0]);
	}	
	
	public void testSingleTermAtBeginning() {
		TermList tl = new TermList();
		Term t1 = new Term("abinash",Term.TERMLOC_BODY,(byte)1,12);
		t1.setDocumentPosition((short)1001);
		t1.setDocumentTypeCode((byte) 44);
		t1.setTermWeight((byte)91);
		
		Term t2 = new Term("abinash",Term.TERMLOC_BODY,(byte)1,22);
		t2.setDocumentTypeCode((byte) 45);
		t2.setTermWeight((byte)92);
		t2.setDocumentPosition((short)1002);
		
		Term t3 = new Term("avinash",Term.TERMLOC_BODY,(byte)1,22);
		t3.setDocumentTypeCode((byte) 45);
		t3.setTermWeight((byte)92);
		t3.setDocumentPosition((short)1002);

		tl.add(t1);
		tl.add(t2);
		tl.add(t3);
		byte[] deletedB = InvertedIndex.delete(tl.toBytes(), (short) 1001);
		assertNotNull(deletedB);
		List<InvertedIndex> iiL = InvertedIndex.read(deletedB);
		assertNotNull(iiL);
		assertEquals(2, iiL.size());
		assertEquals("abinash".hashCode(), iiL.get(0).hash);
		assertEquals(1, iiL.get(0).docPos.length );
		assertEquals(1, iiL.get(0).dtc.length );
		assertEquals(1, iiL.get(0).ttc.length );
		assertEquals(1, iiL.get(0).tw.length );

		assertEquals((short)1002, iiL.get(0).docPos[0]);
		assertEquals((byte)45, iiL.get(0).dtc[0]);
		assertEquals((byte)1, iiL.get(0).ttc[0]);
		assertEquals((byte)92, iiL.get(0).tw[0]);

		assertEquals("avinash".hashCode(), iiL.get(1).hash);
		assertEquals((short)1002, iiL.get(1).docPos[0]);
		assertEquals((byte)45, iiL.get(1).dtc[0]);
		assertEquals((byte)1, iiL.get(1).ttc[0]);
		assertEquals((byte)92, iiL.get(1).tw[0]);
	
	}		

	public void testSingleTermAtMiddle() {

		TermList tl = new TermList();
		Term t1 = new Term("abinash",Term.TERMLOC_BODY,(byte)1,12);
		t1.setDocumentPosition((short)1001);
		t1.setDocumentTypeCode((byte) 44);
		t1.setTermWeight((byte)91);
		
		Term t2 = new Term("abinash",Term.TERMLOC_BODY,(byte)1,22);
		t2.setDocumentTypeCode((byte) 45);
		t2.setTermWeight((byte)92);
		t2.setDocumentPosition((short)1002);
		
		Term t3 = new Term("abinash",Term.TERMLOC_BODY,(byte)1,22);
		t3.setDocumentTypeCode((byte) 46);
		t3.setTermWeight((byte)93);
		t3.setDocumentPosition((short)1003);

		tl.add(t1);
		tl.add(t2);
		tl.add(t3);
		byte[] deletedB = InvertedIndex.delete(tl.toBytes(), (short) 1002);
		assertNotNull(deletedB);
		List<InvertedIndex> iiL = InvertedIndex.read(deletedB);
		assertEquals(1, iiL.size());
		assertEquals((short)1001, iiL.get(0).docPos[0]);
		assertEquals((short)1003, iiL.get(0).docPos[1]);
	
		assertEquals((byte)44, iiL.get(0).dtc[0]);
		assertEquals((byte)46, iiL.get(0).dtc[1]);
	}		
	
	public void testMultipleTermAtMiddle() {

		TermList tl = new TermList();
		Term t1 = new Term("abinash",Term.TERMLOC_BODY,(byte)1,12);
		t1.setDocumentPosition((short)1001);
		t1.setDocumentTypeCode((byte) 44);
		t1.setTermWeight((byte)91);
		
		Term t2 = new Term("avinash",Term.TERMLOC_BODY,(byte)1,22);
		t2.setDocumentTypeCode((byte) 45);
		t2.setTermWeight((byte)92);
		t2.setDocumentPosition((short)1001);
		
		Term t3 = new Term("abinash",Term.TERMLOC_BODY,(byte)1,22);
		t3.setDocumentTypeCode((byte) 46);
		t3.setTermWeight((byte)93);
		t3.setDocumentPosition((short)1002);

		Term t4 = new Term("alinash",Term.TERMLOC_BODY,(byte)1,22);
		t4.setDocumentTypeCode((byte) 46);
		t4.setTermWeight((byte)93);
		t4.setDocumentPosition((short)1003);

		Term t5 = new Term("akinash",Term.TERMLOC_BODY,(byte)1,22);
		t5.setDocumentTypeCode((byte) 46);
		t5.setTermWeight((byte)93);
		t5.setDocumentPosition((short)1004);
		
		tl.add(t1);
		tl.add(t2);
		tl.add(t3);
		tl.add(t4);
		tl.add(t5);
		byte[] deletedB = InvertedIndex.delete(tl.toBytes(), (short) 1002);
		assertNotNull(deletedB);
		
		List<InvertedIndex> iiL = InvertedIndex.read(deletedB);
		assertEquals(4, iiL.size());
		
		assertEquals("akinash".hashCode(), iiL.get(3).hash);
		assertEquals((short)1004, iiL.get(3).docPos[0]);
		
		assertEquals("alinash".hashCode(), iiL.get(0).hash);
		assertEquals((short)1003, iiL.get(0).docPos[0]);

		assertEquals("abinash".hashCode(), iiL.get(1).hash);
		assertEquals((short)1001, iiL.get(1).docPos[0]);

		assertEquals("avinash".hashCode(), iiL.get(2).hash);
		assertEquals((short)1001, iiL.get(1).docPos[0]);
	}		


}
