package com.vg.hbase.comm.manager;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;

public class HBaseCellVersions implements Serializable {
    
    /**
     * 
     */
    private static final long serialVersionUID = -7230902164364243166L;
    private List<HBaseCell> cellVersions = new ArrayList<HBaseCell>();
    private byte[] rowKey;
    
    public HBaseCellVersions(Result resultOb, byte[] coloumnQualifier, byte[] coloumnFamily) {
	HBaseCell singleCell;
	this.rowKey = resultOb.getRow();
	List<KeyValue> columnVersions = resultOb.getColumn(coloumnFamily, coloumnQualifier);
	
	for (int i = columnVersions.size() - 1; i >= 0; i--) {
	    
	    singleCell = new HBaseCell(rowKey, coloumnFamily, coloumnQualifier, columnVersions.get(i).getValue());
	    getCellVersions().add(singleCell);
	}
    }
    
    public List<HBaseCell> getCellVersions() {
	return cellVersions;
    }
    
    public void setCellVersions(List<HBaseCell> cellVersions) {
	this.cellVersions = cellVersions;
    }
}
