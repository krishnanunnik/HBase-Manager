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
    
}
