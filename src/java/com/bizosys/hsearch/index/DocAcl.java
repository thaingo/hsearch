package com.bizosys.hsearch.index;

import java.util.List;

import com.bizosys.oneline.ApplicationFault;

import com.bizosys.hsearch.common.HDocument;
import com.bizosys.hsearch.common.IStorable;
import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.filter.Access;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.schema.IOConstants;

/**
 * An empty ACL only contains 4 bytes.
 * @author karan
 *
 */
public class DocAcl implements IDimension, IStorable {

	/**
	 * Who has edit access to the document
	 */
	public Access editAcl = null;
	
	/**
	 * Who has view access to the document
	 */
	public Access viewAcl = null;
	
	public DocAcl(Access editAcl, Access viewAcl) {
		this.editAcl = editAcl;
		this.viewAcl = viewAcl;
	}
	
	public DocAcl(HDocument aDoc) {
		this.editAcl = aDoc.editAcl;
		this.viewAcl = aDoc.viewAcl;
	}
	
	
	/**
	 * Read the ACL information from the byte array.
	 * Deserialize and initiate
	 * @param bytes : Serialized bytes
	 */
	public DocAcl(byte[] bytes) {
		int pos = 0;
		
		short len = Storable.getShort(pos, bytes);
		pos = pos + 2;
		if ( 0 != len ) {
			byte[] editAclB = new byte[len];
			System.arraycopy(bytes, pos, editAclB, 0, len);
			this.editAcl = new Access(editAclB);
			pos = pos + editAclB.length;
		}

		len = Storable.getShort(pos, bytes);
		pos = pos + 2;
		if ( 0 != len ) {
			byte[] viewAclB = new byte[len];
			System.arraycopy(bytes, pos, viewAclB, 0, len);
			this.viewAcl = new Access(viewAclB);
			pos = pos+ viewAclB.length;
		}
	}

	public byte[] toBytes() {
		boolean isEditAcl = false;
		
		byte[] editAclB = null;
		if ( null != this.editAcl ) {
			isEditAcl = true;
			editAclB = this.editAcl.getAccessList().toBytes();
		}
		
		boolean isViewAcl = false;
		byte[] viewAclB = null;
		if ( null != this.viewAcl ) {
			isViewAcl = true;
			viewAclB = this.viewAcl.getAccessList().toBytes();
		}
		
		int totalBytes = 4;
		if ( isEditAcl  ) totalBytes = totalBytes + editAclB.length;
		if ( isViewAcl  ) totalBytes = totalBytes + viewAclB.length;
		
		byte[] bytes = new byte[totalBytes];
		int pos = 0;
		
		short editAclLen = ( isEditAcl) ? (short)editAclB.length : (short) 0;
		System.arraycopy(Storable.putShort(editAclLen), 0, bytes, pos, 2);
		pos = pos + 2;
		if ( isEditAcl) {
			System.arraycopy(editAclB, 0, bytes, pos, editAclLen);
			pos = pos+ editAclLen;
		}
		
		short viewAclLen = ( isEditAcl) ? (short)viewAclB.length : (short) 0;
		System.arraycopy(Storable.putShort(viewAclLen), 0, bytes, pos, 2);
		pos = pos + 2;
		if (isViewAcl) {
			System.arraycopy(editAclB, 0, bytes, pos, viewAclLen);
			pos = pos+ viewAclLen;
		}
		
		return bytes;
	}

	public void toNVs(List<NV> nvs) throws ApplicationFault {
		nvs.add(new NV(IOConstants.SEARCH_BYTES,IOConstants.ACL_BYTES, this));
	}

	public void cleanup() {
		this.editAcl = null;
		this.viewAcl = null;
	}
	
	public static void main(String[] args) {
		DocAcl acl = new DocAcl(null,null);
		System.out.println( "Empty ACL = " + acl.toBytes().length);
	}
}
