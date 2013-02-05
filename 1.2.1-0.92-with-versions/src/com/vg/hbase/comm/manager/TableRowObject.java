package com.vg.hbase.comm.manager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.vg.hbase.manager.ui.HBaseManagerViewTable;

public class TableRowObject {
    
    private String[] coloumns;
    
    private List<String> coloumnList = new ArrayList<String>();
    private Map<Long, String[][]> coloumnData = new HashMap<Long, String[][]>();
    private List<String[][]> coloumnsDataList = new ArrayList<String[][]>();
    
    List<Long> timestamps = new ArrayList<Long>();
    
    private Result resultOb;
    private String colFamily;
    private Object[] all;
    
    public TableRowObject(Result resultObject, String coloumnFamily, boolean needVersion) {
	resultOb = resultObject;
	colFamily = coloumnFamily;
	if (needVersion) {
	    createVersionDataObjects();
	} else {
	    createNoVersionDataObjects();
	}
	
    }
    
    private Object[] createNoVersionDataObjects() {
	
	NavigableMap<byte[], NavigableMap<byte[], byte[]>> resultMap = resultOb.getNoVersionMap();
	all = new Object[2];
	
	byte[] key = Bytes.toBytes(colFamily);
	
	NavigableMap<byte[], byte[]> innerMap = null;
	innerMap = resultMap.get(key);
	
	if (innerMap == null) {
	    return null;
	}
	coloumns = new String[innerMap.size() + 1];
	String[][] data = new String[innerMap.size() + 1][2];
	
	byte[] innerKey = innerMap.firstKey();
	coloumns[0] = "Row Key";
	data[0][0] = Bytes.toString(resultOb.getRow());
	data[0][1] = "Row Key";
	int i = 1;
	while (innerKey != null) {
	    coloumns[i] = Bytes.toString(innerKey);
	    
	    if (HBaseManagerViewTable.coloumnTypeList.containsKey(coloumns[i])) {
		byte[] value = resultOb.getValue(key, innerKey);
		String action = HBaseManagerViewTable.coloumnTypeList.get(coloumns[i]);
		
		System.out.print("Converting to type:" + action);
		
		data[i][0] = HBaseTableManager.getConvertedValue(value, action, coloumns[i]);
	    } else {
		data[i][0] = Bytes.toString(resultOb.getValue(key, innerKey));
	    }
	    data[i][1] = Bytes.toString(innerKey);
	    
	    i++;
	    innerKey = innerMap.higherKey(innerKey);
	}
	
	all[0] = coloumns;
	all[1] = data;
	
	return data;
	
    }
    
    private void createVersionDataObjects() {
	
	if (resultOb != null && !resultOb.isEmpty()) {
	    NavigableMap<byte[], NavigableMap<byte[], byte[]>> resultMap = resultOb.getNoVersionMap();
	    byte[] key = Bytes.toBytes(colFamily);
	    byte[] innerKey;
	    String coloumn;
	    NavigableMap<byte[], byte[]> innerMap = null;
	    innerMap = resultMap.get(key);
	    Map<String, List<KeyValue>> versionColoumns = new HashMap<String, List<KeyValue>>();
	    List<List<KeyValue>> listVersionColoumns = new ArrayList<List<KeyValue>>();
	    List<KeyValue> values;
	    innerKey = innerMap.firstKey();
	    while (innerKey != null) {
		coloumnList.add(Bytes.toString(innerKey));
		innerKey = innerMap.higherKey(innerKey);
	    }
	    
	    Iterator<String> coloumnIterator = coloumnList.iterator();
	    while (coloumnIterator.hasNext()) {
		coloumn = coloumnIterator.next();
		values = this.resultOb.getColumn(Bytes.toBytes(this.colFamily), Bytes.toBytes(coloumn));
		listVersionColoumns.add(values);
		versionColoumns.put(coloumn, values);
		
	    }
	    
	    all = new Object[3];
	    all[0] = coloumnList;
	    all[1] = versionColoumns;
	    all[2] = listVersionColoumns;
	}
	
    }
    
    public String[] getColoumns() {
	return coloumns;
    }
    
    public List<String[][]> getColoumnData() {
	return coloumnsDataList;
    }
    
    public void resetRowObject() {
	this.coloumnData.clear();
	this.coloumnList.clear();
	this.coloumnsDataList.clear();
	
    }
    
    public Object[] getAllRowInfo() {
	return all;
    }
    
    public static List<byte[]> getContainedQualifiers(Result resultOb, byte[] coloumnFamily) {
	byte[] innerKey;
	List<byte[]> coloumnList = new ArrayList<byte[]>();
	NavigableMap<byte[], byte[]> innerMap = null;
	if (resultOb != null && !resultOb.isEmpty()) {
	    NavigableMap<byte[], NavigableMap<byte[], byte[]>> resultMap = resultOb.getNoVersionMap();
	    byte[] key = coloumnFamily;
	    
	    innerMap = resultMap.get(key);
	    if (innerMap != null) {
		innerKey = innerMap.firstKey();
		while (innerKey != null) {
		    coloumnList.add(innerKey);
		    innerKey = innerMap.higherKey(innerKey);
		}
	    }
	    
	}
	return coloumnList;
    }
}
