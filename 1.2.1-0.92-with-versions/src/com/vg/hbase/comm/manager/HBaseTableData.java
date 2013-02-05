package com.vg.hbase.comm.manager;

import java.io.Serializable;
import java.util.ArrayList;
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
    private List<HBaseRow> hbaseTableData = new ArrayList<HBaseRow>();
    
    public List<HBaseRow> getHbaseTableData() {
	return hbaseTableData;
    }
    
    public void setHbaseTableData(List<HBaseRow> hbaseTableData) {
	this.hbaseTableData = hbaseTableData;
    }
    
}