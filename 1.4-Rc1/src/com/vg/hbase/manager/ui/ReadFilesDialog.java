/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.vg.hbase.manager.ui;

import java.awt.event.ActionEvent;
import java.io.File;
import java.util.List;

import javax.swing.JOptionPane;
import javax.swing.WindowConstants;

import com.vg.hbase.comm.manager.HbaseTableObject;

/**
 * 
 * @author skrishnanunni
 */
public class ReadFilesDialog extends javax.swing.JDialog {

	/**
	 * Creates new form WritingFilesDialog
	 */

	BackupRestoreFileUtil util;
	final ReadFilesDialog dialog;

	public ReadFilesDialog(java.awt.Frame parent, boolean modal, BackupRestoreFileUtil util) {

		super(parent, modal);
		initComponents();
		this.util = util;
		dialog = this;
		// this.setDefaultCloseOperation(WindowConstants.DO_NOTHING_ON_CLOSE);
		this.setVisible(true);
		// scanBackupFiles(util);
	}

	public void scanBackupFiles(BackupRestoreFileUtil util) {

		List<HbaseTableObject> hbObjects = util.restoreFromFiles(this);
		HbaseDataBackupRestoreDialog.setRestoredTableData(hbObjects, dialog);

	}

	/**
	 * This method is called from within the constructor to initialize the form.
	 * WARNING: Do NOT modify this code. The content of this method is always
	 * regenerated by the Form Editor.
	 */
	@SuppressWarnings("unchecked")
	// <editor-fold defaultstate="collapsed"
	// desc="Generated Code">//GEN-BEGIN:initComponents
	private void initComponents() {

		jPanel1 = new javax.swing.JPanel();
		jLabel1 = new javax.swing.JLabel();
		buttonStart = new javax.swing.JButton();
		buttonStart.setText("Start");
		buttonStart.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {

				buttonStartActionPerformed(evt);
				scanBackupFiles(util);
				dialog.setVisible(false);

			}

		});

		// setDefaultCloseOperation(javax.swing.WindowConstants.DISPOSE_ON_CLOSE);

		setTitle("Backup Notification");

		jPanel1.setBorder(javax.swing.BorderFactory.createLineBorder(new java.awt.Color(0, 0, 0)));
		jPanel1.setToolTipText("Backup Progress");

		jLabel1.setFont(new java.awt.Font("SimHei", 0, 24)); // NOI18N
		jLabel1.setText("Scan Backup Files");

		jPanel1.setBorder(javax.swing.BorderFactory.createLineBorder(new java.awt.Color(0, 0, 0)));
		jPanel1.setToolTipText("Backup Progress");

		jLabel1.setFont(new java.awt.Font("SimHei", 0, 24)); // NOI18N
		jLabel1.setText("Scanning Backup Files...");

		buttonStart.setText("Start");

		javax.swing.GroupLayout jPanel1Layout = new javax.swing.GroupLayout(jPanel1);
		jPanel1.setLayout(jPanel1Layout);
		jPanel1Layout.setHorizontalGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING).addGroup(jPanel1Layout.createSequentialGroup().addContainerGap().addComponent(jLabel1, javax.swing.GroupLayout.PREFERRED_SIZE, 491, javax.swing.GroupLayout.PREFERRED_SIZE).addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED).addComponent(buttonStart, javax.swing.GroupLayout.PREFERRED_SIZE, 113, javax.swing.GroupLayout.PREFERRED_SIZE).addContainerGap(73, Short.MAX_VALUE)));
		jPanel1Layout.setVerticalGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING).addGroup(jPanel1Layout.createSequentialGroup().addGap(35, 35, 35).addGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE).addComponent(jLabel1).addComponent(buttonStart)).addContainerGap(51, Short.MAX_VALUE)));

		javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
		getContentPane().setLayout(layout);
		layout.setHorizontalGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING).addGroup(layout.createSequentialGroup().addContainerGap().addComponent(jPanel1, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE).addContainerGap(26, Short.MAX_VALUE)));
		layout.setVerticalGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING).addGroup(layout.createSequentialGroup().addContainerGap().addComponent(jPanel1, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE).addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)));

		pack();

	}// </editor-fold>//GEN-END:initComponents

	private void buttonStartActionPerformed(ActionEvent evt) {

		jLabel1.setText("Scanning Backup Files... Please Wait");
		buttonStart.setVisible(false);

		dialog.setDefaultCloseOperation(WindowConstants.DO_NOTHING_ON_CLOSE);
		this.addWindowListener(new java.awt.event.WindowAdapter() {

			@Override
			public void windowClosing(java.awt.event.WindowEvent e) {

				JOptionPane.showMessageDialog(dialog, "Busy! Please Wait", "Oops!", JOptionPane.ERROR_MESSAGE);
			}
		});

	}

	/**
	 * @param args
	 *            the command line arguments
	 */
	public static void main(String args[]) {

		/*
		 * Set the Nimbus look and feel
		 */
		// <editor-fold defaultstate="collapsed"
		// desc=" Look and feel setting code (optional) ">
		/*
		 * If Nimbus (introduced in Java SE 6) is not available, stay with the
		 * default look and feel. For details see
		 * http://download.oracle.com/javase
		 * /tutorial/uiswing/lookandfeel/plaf.html
		 */
		try {
			for (javax.swing.UIManager.LookAndFeelInfo info : javax.swing.UIManager.getInstalledLookAndFeels()) {
				if ("Nimbus".equals(info.getName())) {
					javax.swing.UIManager.setLookAndFeel(info.getClassName());
					break;
				}
			}
		}
		catch (ClassNotFoundException ex) {
			java.util.logging.Logger.getLogger(ReadFilesDialog.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
		}
		catch (InstantiationException ex) {
			java.util.logging.Logger.getLogger(ReadFilesDialog.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
		}
		catch (IllegalAccessException ex) {
			java.util.logging.Logger.getLogger(ReadFilesDialog.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
		}
		catch (javax.swing.UnsupportedLookAndFeelException ex) {
			java.util.logging.Logger.getLogger(ReadFilesDialog.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
		}
		// </editor-fold>

		/*
		 * Create and display the dialog
		 */
		java.awt.EventQueue.invokeLater(new Runnable() {

			public void run() {

				final ReadFilesDialog dialog = new ReadFilesDialog(new javax.swing.JFrame(), true, new BackupRestoreFileUtil(new File("F:/javax/create.file")));

				// dialog.setVisible(true);
			}
		});
	}

	// Variables declaration - do not modify//GEN-BEGIN:variables
	private javax.swing.JLabel jLabel1;
	private javax.swing.JPanel jPanel1;
	private javax.swing.JButton buttonStart;
	// End of variables declaration//GEN-END:variables
}
