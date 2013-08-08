
package com.vg.hbase.comm.manager;

/*
 * To change this template, choose Tools | Templates and open the template in
 * the editor.
 */

import java.io.Serializable;

/**
 * 
 * @author skrishnanunni
 */
public class HbaseTableObject implements Serializable {

	/**
     * 
     */
	private static final long serialVersionUID = -2311782994212110598L;
	private HBaseTableStructure tableStructure;
	private HBaseTableData tableData;
	private boolean lastObject;
	private boolean linkedFileAvailable;
	private int totalLinkedFiles;

	public HBaseTableData getTableData() {

		return tableData;
	}

	public void setTableData(HBaseTableData tableData) {

		this.tableData = tableData;
	}

	public HBaseTableStructure getTableStructure() {

		return tableStructure;
	}

	public void setTableStructure(HBaseTableStructure tableStructure) {

		this.tableStructure = tableStructure;
	}

	public boolean isLastObject() {

		return lastObject;
	}

	public void setLastObject(boolean lastObject) {

		this.lastObject = lastObject;
	}

	public boolean isLinkedFileAvailable() {

		return linkedFileAvailable;
	}

	public void setLinkedFileAvailable(boolean linkedFileAvailable) {

		this.linkedFileAvailable = linkedFileAvailable;
	}

	public int getTotalLinkedFiles() {

		return totalLinkedFiles;
	}

	public void setTotalLinkedFiles(int totalLinkedFiles) {

		this.totalLinkedFiles = totalLinkedFiles;
	}

}
