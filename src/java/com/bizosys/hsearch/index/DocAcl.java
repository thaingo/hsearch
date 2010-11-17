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
	public Access viewPermission = null;
	
	/**
	 * Who has view access to the document
	 */
	public Access editPermission = null;
	
	public DocAcl(Access viewAcl, Access editAcl) {
		this.viewPermission = viewAcl;
		this.editPermission = editAcl;
	}
	
	public DocAcl(HDocument aDoc) {
		this.viewPermission = aDoc.viewPermission;
		this.editPermission = aDoc.editPermission;
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
			this.viewPermission = new Access(editAclB);
			pos = pos + editAclB.length;
		}

		len = Storable.getShort(pos, bytes);
		pos = pos + 2;
		if ( 0 != len ) {
			byte[] viewAclB = new byte[len];
			System.arraycopy(bytes, pos, viewAclB, 0, len);
			this.editPermission = new Access(viewAclB);
			pos = pos+ viewAclB.length;
		}
	}

	public byte[] toBytes() {

		boolean isViewPerm = false; byte[] viewPermissionB = null;
		if ( null != this.viewPermission ) {
			isViewPerm = true;
			viewPermissionB = this.viewPermission.toStorable().toBytes();
		}
		boolean isEditPerm = false; byte[] editPermissionB = null;
		if ( null != this.editPermission ) {
			isEditPerm = true;
			editPermissionB = this.editPermission.toStorable().toBytes();
		}
		
		int totalBytes = 4; /** 2 + 2 the short lengths */
		if ( isViewPerm  ) totalBytes = totalBytes + viewPermissionB.length;
		if ( isEditPerm  ) totalBytes = totalBytes + editPermissionB.length;
		
		byte[] bytes = new byte[totalBytes];
		int pos = 0;
		
		short viewPermLen = ( isViewPerm) ? (short)viewPermissionB.length : (short) 0;
		System.arraycopy(Storable.putShort(viewPermLen), 0, bytes, pos, 2);
		pos = pos + 2;
		if ( isViewPerm) {
			System.arraycopy(viewPermissionB, 0, bytes, pos, viewPermLen);
			pos = pos+ viewPermLen;
		}
		
		short editPermLen = ( isEditPerm) ? (short)editPermissionB.length : (short) 0;
		System.arraycopy(Storable.putShort(editPermLen), 0, bytes, pos, 2);
		pos = pos + 2;
		if (isEditPerm) {
			System.arraycopy(editPermissionB, 0, bytes, pos, editPermLen);
			pos = pos+ editPermLen;
		}
		return bytes;
	}

	public void toNVs(List<NV> nvs) throws ApplicationFault {
		nvs.add(new NV(IOConstants.SEARCH_BYTES,IOConstants.ACL_BYTES, this));
	}

	public void cleanup() {
		this.viewPermission = null;
		this.editPermission = null;
	}
	
	public static void main(String[] args) {
		DocAcl acl = new DocAcl(null,null);
		System.out.println( "Empty ACL = " + acl.toBytes().length);
	}
}
