package com.vg.hbase.comm.manager;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

/**
 * 
 * @author skrishnanunni
 */
public class HBaseTableData implements Serializable {
    
    /**
     * 
     */
    private static final long serialVersionUID = 7939170531079486032L;
    // ResultScanner dbDataSet;
    HashMap<String, String[][]> dbDataList;
    List<byte[]> rowKeylist;
    
    public List<byte[]> getRowKeylist() {
	return rowKeylist;
    }
    
    public void setRowKeylist(List<byte[]> rowKeylist) {
	this.rowKeylist = rowKeylist;
    }
    
    public HashMap<String, String[][]> getDbDataList() {
	return dbDataList;
    }
    
    public void setDbDataList(HashMap<String, String[][]> dbDataList) {
	this.dbDataList = dbDataList;
    }
    
}