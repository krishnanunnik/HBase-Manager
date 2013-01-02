/**
 * 
 */
package com.vg.hbase.comm.manager;

import java.io.IOException;
import java.util.HashMap;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.vg.hbase.manager.ui.HBaseManagerViewTable;
import com.vg.hbase.operations.base.HBaseConfigurationManager;

/**
 * @author kvijay
 * 
 */
public class HBaseTableManager {
    HashMap<String, HTable> tableMap = null;
    private HBaseConfigurationManager hBaseConfigManager;
    private String[] columnQualifiers;
    private String[][] ResultMap;
    
    public String[] getColumnQualifiers() {
	return columnQualifiers;
    }
    
    public void setColumnQualifiers(String[] columnQualifiers) {
	this.columnQualifiers = columnQualifiers;
    }
    
    public String[][] getResultMap() {
	return ResultMap;
    }
    
    public HBaseTableManager() {
	if (hBaseConfigManager == null) {
	    hBaseConfigManager = new HBaseConfigurationManager();
	}
	
	if (tableMap == null) {
	    this.tableMap = new HashMap<String, HTable>();
	}
	createHTableMap();
    }
    
    public HBaseTableManager(String newConf) {
	
	hBaseConfigManager = new HBaseConfigurationManager(newConf);
	
	if (tableMap == null) {
	    this.tableMap = new HashMap<String, HTable>();
	}
	createHTableMap();
    }
    
    private void createHTableMap() {
	
	try {
	    
	    // HBaseTableList hbaseTables[] = HBaseTableList.values();
	    // int hTablesCount = HBaseTableList.values().length;
	    // for(int tableIterator = 0; tableIterator < hTablesCount;
	    // tableIterator ++){
	    this.tableMap.put("LatestId", new HTable(HBaseConfigurationManager.getHbaseConf(), "LatestId"));
	    // }
	}
	catch (IOException e) {
	}
    }
    
    private void addHTableIntoMap(String tableName) {
	try {
	    this.tableMap.put(tableName, new HTable(HBaseConfigurationManager.getHbaseConf(), tableName));
	}
	catch (IOException e) {
	}
    }
    
    public HTable getTable(String tableName) {
	HTable table = tableMap.get(tableName);
	
	if (table == null) {
	    addHTableIntoMap(tableName);
	    table = tableMap.get(tableName);
	}
	
	return table;
    }
    
    public String[] getColFamilies(HTable userTable) {
	String[] families = null;
	try {
	    Set<byte[]> familySet = userTable.getTableDescriptor().getFamiliesKeys();
	    
	    Object[] allFamiles = familySet.toArray();
	    families = new String[allFamiles.length];
	    int i = 0;
	    for (Object family2 : allFamiles) {
		byte[] family = (byte[]) family2;
		// userTable.getTableDescriptor().getFamily(family).getMaxVersions();
		families[i] = Bytes.toString(family);
		i++;
	    }
	    
	}
	catch (IOException e) {
	    System.out.println("Exception :" + e);
	}
	
	return families;
    }
    
    public String[] getColFamilyVersions(HTable userTable) {
	String[] families = null;
	try {
	    Set<byte[]> familySet = userTable.getTableDescriptor().getFamiliesKeys();
	    int maxversions;
	    Object[] allFamiles = familySet.toArray();
	    families = new String[allFamiles.length];
	    int i = 0;
	    for (Object family2 : allFamiles) {
		byte[] family = (byte[]) family2;
		maxversions = userTable.getTableDescriptor().getFamily(family).getMaxVersions();
		families[i] = String.valueOf(maxversions);
		i++;
	    }
	    
	}
	catch (IOException e) {
	    System.out.println("Exception :" + e);
	}
	
	return families;
    }
    
    public static String[] getAllTableNames() {
	String[] tblnames = null;
	try {
	    HTableDescriptor[] tables = HBaseConfigurationManager.getHbaseAdmin().listTables();
	    tblnames = new String[tables.length];
	    int i = 0;
	    for (HTableDescriptor tbl : tables) {
		tblnames[i] = tbl.getNameAsString();
		i++;
	    }
	}
	catch (IOException e) {
	    Logger.getLogger(HBaseTableManager.class).log(Level.ERROR, ExceptionUtils.getFullStackTrace(e));
	}
	
	return tblnames;
    }
    
    public boolean getDataFiller(Result resultObject, String colFamily) {
	String[] coloumns = null;
	String[][] data = null;
	String action = "";
	
	/*
	 * getting the Map of map containing the the column family, qualifier
	 * and content info for each row in the result object
	 */
	
	if (!resultObject.isEmpty()) {
	    
	    NavigableMap<byte[], NavigableMap<byte[], byte[]>> resultMap = resultObject.getNoVersionMap();
	    byte[] key = Bytes.toBytes(colFamily);
	    
	    NavigableMap<byte[], byte[]> innerMap = null;
	    innerMap = resultMap.get(key);
	    
	    if (innerMap == null) {
		return false;
	    }
	    coloumns = new String[innerMap.size() + 1];
	    data = new String[innerMap.size() + 1][2];
	    
	    byte[] innerKey = innerMap.firstKey();
	    coloumns[0] = "Row Key";
	    data[0][0] = Bytes.toString(resultObject.getRow());
	    data[0][1] = "Row Key";
	    int i = 1;
	    while (innerKey != null) {
		coloumns[i] = Bytes.toString(innerKey);
		/*
		 * if(coloumns[i].equals("id")) {
		 * HBaseManagerViewTable.coloumnTypeList.put("id", "LONG"); }
		 */
		
		if (HBaseManagerViewTable.coloumnTypeList.containsKey(coloumns[i])) {
		    byte[] value = resultObject.getValue(key, innerKey);
		    action = HBaseManagerViewTable.coloumnTypeList.get(coloumns[i]);
		    
		    System.out.print("Converting to type:" + action);
		    
		    data[i][0] = getConvertedValue(value, action, coloumns[i]);
		} else {
		    data[i][0] = Bytes.toString(resultObject.getValue(key, innerKey));
		}
		data[i][1] = Bytes.toString(innerKey);
		
		i++;
		innerKey = innerMap.higherKey(innerKey);
	    }
	}
	this.columnQualifiers = coloumns;
	this.ResultMap = data;
	return true;
    }
    
    private String getConvertedValue(byte[] value, String action, String coloumn) {
	
	String cnvValue = "";
	try {
	    
	    if (action.equals("INTEGER")) {
		cnvValue = String.valueOf(Bytes.toInt(value));
	    } else if (action.equals("DOUBLE")) {
		cnvValue = String.valueOf(Bytes.toDouble(value));
	    } else if (action.equals("SHORT")) {
		cnvValue = String.valueOf(Bytes.toShort(value));
	    } else if (action.equals("LONG")) {
		cnvValue = String.valueOf(Bytes.toLong(value));
	    } else if (action.equals("BOOLEAN")) {
		cnvValue = String.valueOf(Bytes.toBoolean(value));
	    }
	}
	catch (Exception e) {
	    System.out.println("InConvertible");
	    HBaseManagerViewTable.coloumnTypeList.remove(coloumn);
	}
	return cnvValue;
	
    }
    
    public static byte[] getConvertedValue(String value, String action, String coloumn) {
	
	byte[] cnvValue = "".getBytes();
	
	try {
	    if (action.equals("INTEGER")) {
		cnvValue = Bytes.toBytes(Integer.parseInt(value));
	    } else if (action.equals("DOUBLE")) {
		cnvValue = Bytes.toBytes(Double.parseDouble(value));
	    } else if (action.equals("SHORT")) {
		cnvValue = Bytes.toBytes(Short.parseShort(value));
	    } else if (action.equals("LONG")) {
		cnvValue = Bytes.toBytes(Long.parseLong(value));
	    } else if (action.equals("BOOLEAN")) {
		cnvValue = Bytes.toBytes(Boolean.parseBoolean(value));
	    }
	}
	catch (Exception e) {
	    System.out.println("InConvertible");
	    HBaseManagerViewTable.coloumnTypeList.remove(coloumn);
	}
	return cnvValue;
	
    }
}
