package com.vg.hbase.comm.manager;

import java.io.Serializable;

public class HBaseCell implements Serializable {
    
    /**
     * 
     */
    private static final long serialVersionUID = -8367790555462692931L;
    private byte[] coloumnQualifier;
    
    public byte[] getColoumnQualifier() {
	return coloumnQualifier;
    }
    
    public byte[] getColoumnValue() {
	return coloumnValue;
    }
    
    public byte[] getColoumnFamily() {
	return coloumnFamily;
    }
    
    public byte[] getRowKey() {
	return rowKey;
    }
    
    private byte[] coloumnValue;
    private byte[] coloumnFamily;
    private byte[] rowKey;
    
    public HBaseCell(byte[] rowKey, byte[] coloumnFamily, byte[] coloumnQualifier, byte[] coloumnValue) {
	
	this.coloumnQualifier = coloumnQualifier;
	this.coloumnValue = coloumnValue;
	this.coloumnFamily = coloumnFamily;
	this.rowKey = rowKey;
    }
    
}
