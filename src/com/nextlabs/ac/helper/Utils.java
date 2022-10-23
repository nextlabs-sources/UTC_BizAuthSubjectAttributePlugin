package com.nextlabs.ac.helper;



import java.io.BufferedOutputStream;

import java.io.File;

import java.io.FileInputStream;

import java.io.FileOutputStream;

import java.io.IOException;

import java.io.InputStream;

import java.io.ObjectInputStream;

import java.io.ObjectOutputStream;

import java.sql.Timestamp;

import java.util.Enumeration;

import java.util.List;

import java.util.zip.ZipEntry;

import java.util.zip.ZipException;

import java.util.zip.ZipFile;

import java.util.zip.ZipOutputStream;



import org.apache.commons.logging.Log;

import org.apache.commons.logging.LogFactory;



public class Utils {

	private static final Log LOG = LogFactory.getLog(Utils.class);



	public static void writeData(Object data, String path) throws IOException {

		File file = new File(path);

		if (file.exists()) {

			file.delete();

		}

		file.createNewFile();

		ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(

				file));

		out.writeObject(data);

		out.close();

	}



	public static Object readData(String path) {

		File file = new File(path);

		Object obj = null;

		try {

			if (file.exists()) {

				FileInputStream fin = new FileInputStream(path);

				ObjectInputStream ois = new ObjectInputStream(fin);

				obj = ois.readObject();

				ois.close();

			}

		} catch (IOException e) {

			LOG.error(" Utils readData() error: ", e);

			obj = null;



		} catch (ClassNotFoundException e) {

			LOG.error(" Utils readData() error: ", e);

			obj = null;

		}

		return obj;

	}



	public static void compressFiles(List<String> entries, String zipFile) {



		byte[] buffer = new byte[1024];



		try {

			FileOutputStream fos = new FileOutputStream(zipFile);

			ZipOutputStream zos = new ZipOutputStream(fos);



			for (String zipEntry : entries) {

				File f = new File(zipEntry);

				ZipEntry ze = new ZipEntry(f.getName());

				zos.putNextEntry(ze);

				FileInputStream in = new FileInputStream(f);

				int len;

				while ((len = in.read(buffer)) > 0) {

					zos.write(buffer, 0, len);

				}



				in.close();

				zos.closeEntry();

			}



			// remember close it

			zos.close();



		} catch (IOException e) {

			LOG.error(" Utils compressFiles() error: ", e);

		}



	}



	public static void UncompressFiles(String extractTo, File zipFile) {

		try {

			ZipFile zip = new ZipFile(zipFile);

			Enumeration<ZipEntry> entries = (Enumeration<ZipEntry>) zip

					.entries();

			while (entries.hasMoreElements()) {

				ZipEntry entry = (ZipEntry) entries.nextElement();

				File file = new File(extractTo, entry.getName());

				InputStream in = zip.getInputStream(entry);

				BufferedOutputStream out = new BufferedOutputStream(

						new FileOutputStream(file));

				byte[] buffer = new byte[8192];

				int read;

				while (-1 != (read = in.read(buffer))) {

					out.write(buffer, 0, read);

				}

				in.close();

				out.close();

			}

			zipFile.delete();

		} catch (ZipException e) {

			LOG.error("Exception in uncompressing the file:", e);

		} catch (IOException e) {

			LOG.error("Exception in uncompressing the file:", e);

		}

		



	}



	public static Timestamp getFileModifiedDate(String path) {

		File file = new File(path);

		Timestamp modifiedDate = null;

		if (file.exists()) {

			modifiedDate = new Timestamp(file.lastModified());

		}

		return modifiedDate;

	}



}

