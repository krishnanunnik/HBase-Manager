package com.vg.hbase.comm.manager;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

public class HBaseRow implements Serializable {
    
    /**
     * 
     */
    private static final long serialVersionUID = 2666078248584185425L;
    
    private List<HBaseCellVersions> rowWithAllVersions = new ArrayList<HBaseCellVersions>();
    
    public HBaseRow(Result result, byte[][] coloumnFamily) {
	
	List<byte[]> coloumnList;
	HBaseCellVersions versions;
	for (byte[] colFamily : coloumnFamily) {
	    
	    coloumnList = TableRowObject.getContainedQualifiers(result, colFamily);
	    Iterator<byte[]> coloumnQualifierList = coloumnList.iterator();
	    while (coloumnQualifierList.hasNext()) {
		versions = new HBaseCellVersions(result, coloumnQualifierList.next(), colFamily);
		rowWithAllVersions.add(versions);
	    }
	}
	
    }
    
    public void putRow(HTable table) throws IOException {
	List<Put> putList = new ArrayList<Put>();
	Iterator<HBaseCellVersions> iterateRows = this.rowWithAllVersions.iterator();
	HBaseCellVersions cellVersions = null;
	HBaseCell cell;
	while (iterateRows.hasNext()) {
	    cellVersions = iterateRows.next();
	    List<HBaseCell> cells = cellVersions.getCellVersions();
	    
	    Iterator<HBaseCell> cellIterator = cells.iterator();
	    while (cellIterator.hasNext()) {
		cell = cellIterator.next();
		putList.add(getPutObject(cell));
		
	    }
	    
	}
	table.put(putList);
	
    }
    
    private Put getPutObject(HBaseCell cell) {
	
	Put cellPut = new Put(cell.getRowKey());
	cellPut.add(cell.getColoumnFamily(), cell.getColoumnQualifier(), cell.getColoumnValue());
	return cellPut;
    }
    
}
