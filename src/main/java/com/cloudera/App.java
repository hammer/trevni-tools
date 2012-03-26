package com.cloudera;

import java.io.File;
import java.io.FileReader;
import java.io.FileOutputStream;
import java.util.Random;

import au.com.bytecode.opencsv.CSVReader;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import org.apache.trevni.ColumnFileMetaData;
import org.apache.trevni.ColumnFileReader;
import org.apache.trevni.ColumnFileWriter;
import org.apache.trevni.ColumnMetaData;
import org.apache.trevni.ValueType;

public class App {
    private static String codec = "snappy";
    private static String checksum = "crc32";

    private static ColumnFileMetaData createFileMeta() {
        return new ColumnFileMetaData().setCodec(codec).setChecksum(checksum);
    }

    public static long CSVToTrevni(String csvFilename, String trevniFilename) throws Exception {
	File trevniFile = new File(trevniFilename);
	trevniFile.delete();

	// Get first line of table
	CSVReader reader = new CSVReader(new FileReader(csvFilename));
	String[] firstLine = reader.readNext();

	// Use first line of table to initialize Trevni file writer
	ColumnMetaData[] cols;
	cols = new ColumnMetaData[firstLine.length];
	for (int i = 0; i < firstLine.length; i++) {
	    cols[i] = new ColumnMetaData("col" + i, ValueType.STRING);
	}
	ColumnFileWriter out = new ColumnFileWriter(createFileMeta(), cols);
	out.writeRow(firstLine);

	// Iterate through the CSV and write each row to Trevni file
	String[] nextLine;
	while ((nextLine = reader.readNext()) != null) {
	    out.writeRow(nextLine);
	}
	out.writeTo(trevniFile);

	// Count the number of rows written
	ColumnFileReader in = new ColumnFileReader(trevniFile);
	return in.getRowCount();
    }

    public static void avscToJSON(String avscFilename, String jsonFilename) throws Exception {
	// read in the Avro schema
	File avscFile = new File(avscFilename);
	Parser p = new Parser();
	Schema s = p.parse(avscFile);

	// write some random data in JSON
	GenericDatumWriter<Object> writer = new GenericDatumWriter<Object>(s);
	Encoder e = EncoderFactory.get().jsonEncoder(s, new FileOutputStream(new File(jsonFilename)));
	try {
	    for (Object datum : new RandomData(s, 1)) {
		writer.write(datum, e);
	    }
	} finally {
	    e.flush();
	}
    }

    public static void main(String[] args) throws Exception
    {
	//        System.out.println("Rows read: " + CSVToTrevni(args[0], args[1]));
	avscToJSON(args[0], args[1]);
    }
}
