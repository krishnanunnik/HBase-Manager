
package com.vg.hbase.manager.ui;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.swing.JDialog;
import javax.swing.JOptionPane;

import org.apache.commons.lang.exception.ExceptionUtils;

import com.vg.hbase.comm.manager.HbaseTableObject;

public class BackupRestoreFileUtil {

	private File selectedFile;
	private int filecount = 0;
	private String filePath;

	public static boolean runningNow;

	public BackupRestoreFileUtil(File backupFile) {

		this.selectedFile = backupFile;
		filePath = this.selectedFile.getAbsolutePath();

	}

	private File getNextFile(boolean verifyExistence) {

		if (filecount > 0) {

			this.selectedFile = new File(filePath.concat("_").concat(String.valueOf(filecount)));
			filecount++;

		}
		else {

			filecount++;
		}
		if (verifyExistence) {
			if (this.selectedFile.isFile())
				return this.selectedFile;
			else
				return null;
		}
		else
			return this.selectedFile;

	}

	public void backupToFiles(List<HbaseTableObject> hTables) throws IOException {

		runningNow = true;
		Iterator<HbaseTableObject> iterator = hTables.iterator();
		boolean markedHeader = false;
		while (iterator.hasNext()) {

			try {
				HbaseTableObject next = iterator.next();
				if (!markedHeader)
					next.setTotalLinkedFiles(hTables.size());

				File nextFile = getNextFile(false);
				ObjectOutputStream output = new ObjectOutputStream(new FileOutputStream(nextFile));
				System.out.println("Writing: " + this.selectedFile.getName());

				if (!iterator.hasNext()) {
					System.out.println("Marking link end at " + this.selectedFile);
					next.setLinkedFileAvailable(false);
				}
				output.writeObject(next);
				output.close();
			}
			catch (Throwable e) {
				System.out.println("Exception while backup:  files may not be written properly. " + ExceptionUtils.getFullStackTrace(e));

			}
		}
		runningNow = false;

	}

	public List<HbaseTableObject> restoreFromFiles(JDialog dialog) {

		List<HbaseTableObject> tableObjects = new ArrayList<HbaseTableObject>();
		runningNow = true;

		HbaseTableObject object;

		try {
			File nextFile = getNextFile(true);
			ObjectInputStream instream = new ObjectInputStream(new FileInputStream(nextFile));

			object = (HbaseTableObject) instream.readObject();
			int totalFiles = object.getTotalLinkedFiles();
			int i = 1;
			System.out.println("Total: Files" + totalFiles);
			System.out.println("Scanning backup files..." + i + " of " + totalFiles);
			System.out.println("Reading " + this.selectedFile.getName());

			tableObjects.add(object);
			instream.close();

			while (object.isLinkedFileAvailable()) {
				i++;
				System.out.println("Scanning backup files..." + i + " of " + totalFiles);
				nextFile = getNextFile(true);
				if (nextFile == null) {
					JOptionPane.showMessageDialog(dialog, "All the Backup files not available. \n This restore will work only partial", "Error", JOptionPane.ERROR_MESSAGE);
					break;
				}
				else {
					instream = new ObjectInputStream(new FileInputStream(nextFile));
					System.out.println("Reading " + this.selectedFile.getName());
					object = (HbaseTableObject) instream.readObject();
					instream.close();
					System.out.println("Continue reading: " + object.isLinkedFileAvailable());
					tableObjects.add(object);
				}

			}
			runningNow = false;
			return tableObjects;

		}
		catch (Exception e) {
			System.out.println("Exception while restoring. Restoring failed" + ExceptionUtils.getFullStackTrace(e));

			runningNow = false;
		}

		return null;
	}

}
