package com.vg.hbase.comm.manager;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

import com.vg.hbase.operations.base.HBaseConfigurationManager;

/**
 * 
 * @author skrishnanunni
 */
public class HBaseTableStructure implements Serializable {
    /**
     * 
     */
    private static final long serialVersionUID = 4757579306171103892L;
    HashMap<String, String> tableDescriptor;
    String HTableName;
    
    public String getHTableName() {
	return HTableName;
    }
    
    public void setHTableName(String HTableName) {
	this.HTableName = HTableName;
	
    }
    
    public void createWriteTableStructure(String tableName) {
	try {
	    tableDescriptor = new HashMap<String, String>();
	    this.HTableName = tableName;
	    HTableDescriptor descriptor = HBaseConfigurationManager.hbaseAdmin.getTableDescriptor(Bytes
		    .toBytes(tableName));
	    HColumnDescriptor[] cols = descriptor.getColumnFamilies();
	    for (int i = 0; i < cols.length; i++) {
		tableDescriptor.put(cols[i].getNameAsString(), String.valueOf(cols[i].getMaxVersions()));
		
	    }
	    
	    Logger.getLogger(HBaseTableStructure.class.getName()).log(Level.INFO,
		    "TableName Descriptor Initaed:" + HTableName);
	}
	catch (IOException ex) {
	    Logger.getLogger(HBaseTableStructure.class.getName()).log(Level.SEVERE, null, ex);
	}
    }
    
    public HTableDescriptor createReadTableStructure() {
	HTableDescriptor descriptor = null;
	HColumnDescriptor colDesc = null;
	String[] familyNames = new String[this.tableDescriptor.size()];
	tableDescriptor.keySet().toArray(familyNames);
	int versions;
	try {
	    descriptor = new HTableDescriptor(this.HTableName);
	    
	    for (int i = 0; i < tableDescriptor.size(); i++) {
		colDesc = new HColumnDescriptor(familyNames[i]);
		versions = Integer.parseInt(tableDescriptor.get(familyNames[i]));
		colDesc.setMaxVersions(versions);
		descriptor.addFamily(colDesc);
		
	    }
	    
	    Logger.getLogger(HBaseTableStructure.class.getName()).log(Level.INFO,
		    "TableName Descriptor Initaed:" + HTableName);
	}
	catch (Exception ex) {
	    Logger.getLogger(HBaseTableStructure.class.getName()).log(Level.SEVERE, null, ex);
	}
	return descriptor;
    }
    
    public byte[][] getAllColoumnFamilies() {
	byte[][] familyBytes = null;
	
	Set<String> fams = tableDescriptor.keySet();
	String family[] = new String[fams.size()];
	familyBytes = new byte[fams.size()][];
	fams.toArray(family);
	
	for (int i = 0; i < family.length; i++)
	    familyBytes[i] = Bytes.toBytes(family[i]);
	
	return familyBytes;
	
    }
    
}