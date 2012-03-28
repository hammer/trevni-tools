package com.cloudera;

import java.io.File;
import java.io.FileReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Random;

import au.com.bytecode.opencsv.CSVReader;

import org.apache.commons.io.FilenameUtils;

import com.google.common.base.Joiner;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.StringType;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import org.apache.trevni.ColumnFileMetaData;
import org.apache.trevni.ColumnFileReader;
import org.apache.trevni.ColumnFileWriter;
import org.apache.trevni.ColumnMetaData;
import org.apache.trevni.ValueType;
import org.apache.trevni.avro.AvroShredder;

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

    public static void modifyStringTypes(Schema s) throws Exception {
	switch (s.getType()) {
	case STRING:
	    GenericData.setStringType(s, GenericData.StringType.String);
	    break;
	case ARRAY:
	    modifyStringTypes(s.getElementType());
	    break;
	case MAP:
	    modifyStringTypes(s.getValueType());  // not sure how to get key type
	    break;
	case RECORD:
	    for (Field f : s.getFields()) {
		modifyStringTypes(f.schema());
	    }
	    break;
	case UNION:
	    for (Schema branch_schema : s.getTypes()) {
		modifyStringTypes(branch_schema);
	    }
	    break;
	}
    }

    public static String getFamilyTree(ColumnMetaData col) {
	ArrayList<String> familyTree = new ArrayList<String>();
	ColumnMetaData parent = col.getParent();
	while (parent != null) {
	    familyTree.add(0, parent.getName());
	    parent = parent.getParent();
	}
	Joiner joiner = Joiner.on("->");
	return joiner.join(familyTree);
    }

    public static long jsonToShreddedTrevni(String jsonFilename) throws Exception {
	// get the schema that corresponds to the JSON datum
	String avscFilename = "schemas/" + FilenameUtils.getName(FilenameUtils.removeExtension(jsonFilename)) + ".avsc";
	File avscFile = new File(avscFilename);
	Parser p = new Parser();
	Schema s = p.parse(avscFile);

	// read in the JSON-encoded datum
	modifyStringTypes(s);  // Use String, not Utf8
	GenericDatumReader<Object> reader = new GenericDatumReader<Object>(s);
	Decoder e = DecoderFactory.get().jsonDecoder(s, new FileInputStream(new File(jsonFilename)));
	Object datum = reader.read(null, e);

	// create an AvroShredder instance
	AvroShredder as = new AvroShredder(s, reader.getData());
	ColumnMetaData[] columnized_cols = as.getColumns();
	for (int i = 0; i < columnized_cols.length; i++) {
	    System.out.println("Columnized column " + i);
	    System.out.println("  Name: " + columnized_cols[i].getName());
	    System.out.println("  Type: " + columnized_cols[i].getType());
	    System.out.println("  isArray: " + columnized_cols[i].isArray());
	    System.out.println("  Parent: " + columnized_cols[i].getParent());
	    System.out.println("  Family Tree: " + getFamilyTree(columnized_cols[i]));
	}
	
	// shred some columns!
	ColumnFileWriter out = new ColumnFileWriter(createFileMeta(), as.getColumns());
	as.shred(datum, out);
	String trevniFilename = "data/" + FilenameUtils.getName(FilenameUtils.removeExtension(jsonFilename)) + ".trv";
	File trevniFile = new File(trevniFilename);
	trevniFile.delete();
	out.writeTo(trevniFile);

	// count the number of columns in the shredded table
	ColumnFileReader in = new ColumnFileReader(trevniFile);
	ColumnMetaData[] read_cols = in.getColumnMetaData();
	for (int i = 0; i < read_cols.length; i++) {
	    System.out.println("Column " + i + " is named " + read_cols[i].getName() + " and is of type " + read_cols[i].getType());
	}
	return in.getColumnCount();
    }

    public static void main(String[] args) throws Exception
    {
	System.out.println("Columns shredded: " + jsonToShreddedTrevni(args[0]));
    }
}
