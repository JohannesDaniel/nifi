package org.apache.nifi.xml;

import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.ListRecordSet;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.serialization.record.SchemaIdentifier;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestWriteXMLResultUtils {

    protected static final String FIELD_NAME = "NAME";
    protected static final String FIELD_AGE = "AGE";
    protected static final String FIELD_COUNTRY = "COUNTRY";
    protected static final String FIELD_ADDRESS = "ADDRESS";
    protected static final String FIELD_STREET = "STREET";
    protected static final String FIELD_CITY = "CITY";
    protected static final String FIELD_CHILDREN = "CHILDREN";

    protected static Map<String,Object> RECORD_FIELDS_PERSON_1 = new HashMap<>();
    protected static Map<String,Object> RECORD_FIELDS_PERSON_2 = new HashMap<>();
    protected static Map<String,Object> RECORD_FIELDS_ADDRESS_1 = new HashMap<>();
    protected static Map<String,Object> RECORD_FIELDS_ADDRESS_2 = new HashMap<>();

    static {
        RECORD_FIELDS_PERSON_1.put(FIELD_NAME, "Cleve Butler");
        RECORD_FIELDS_PERSON_1.put(FIELD_AGE, 42);
        RECORD_FIELDS_PERSON_1.put(FIELD_COUNTRY, "USA");
        RECORD_FIELDS_PERSON_2.put(FIELD_NAME, "Ainslie Fletcher");
        RECORD_FIELDS_PERSON_2.put(FIELD_AGE, 33);
        RECORD_FIELDS_PERSON_2.put(FIELD_COUNTRY, "UK");
        RECORD_FIELDS_ADDRESS_1.put(FIELD_STREET, "292 West Street");
        RECORD_FIELDS_ADDRESS_1.put(FIELD_CITY, "Jersey City");
        RECORD_FIELDS_ADDRESS_2.put(FIELD_STREET, "123 6th St.");
        RECORD_FIELDS_ADDRESS_2.put(FIELD_CITY, "Seattle");

        RECORD_FIELDS_PERSON_1 = Collections.unmodifiableMap(RECORD_FIELDS_PERSON_1);
        RECORD_FIELDS_PERSON_2 = Collections.unmodifiableMap(RECORD_FIELDS_PERSON_2);
        RECORD_FIELDS_ADDRESS_1 = Collections.unmodifiableMap(RECORD_FIELDS_ADDRESS_1);
        RECORD_FIELDS_ADDRESS_2 = Collections.unmodifiableMap(RECORD_FIELDS_ADDRESS_2);
    }

    protected static final SchemaIdentifier SCHEMA_IDENTIFIER_PERSON = SchemaIdentifier.builder().name("PERSON").id(0L).version(0).build();
    // protected static final SchemaIdentifier SCHEMA_IDENTIFIER_ADDRESS = SchemaIdentifier.builder().name("ADDRESS").id(0L).version(0).build();


    protected static final String DATE_FORMAT = RecordFieldType.DATE.getDefaultFormat();
    protected static final String TIME_FORMAT = RecordFieldType.TIME.getDefaultFormat();
    protected static final String TIMESTAMP_FORMAT = RecordFieldType.TIMESTAMP.getDefaultFormat();

    /*
    Simple records
     */

    protected static RecordSet getSimpleRecords() {
        RecordSchema schema = getSimpleSchema();

        List<Record> records = new ArrayList<>();
        records.add(new MapRecord(schema, RECORD_FIELDS_PERSON_1));
        records.add(new MapRecord(schema, RECORD_FIELDS_PERSON_2));

        return new ListRecordSet(schema, records);
    }

    protected static RecordSet getSimpleRecordsWithNullValues() {
        RecordSchema schema = getSimpleSchema();

        Map<String, Object> recordWithoutName1 = new HashMap<>(RECORD_FIELDS_PERSON_1);
        Map<String, Object> recordWithoutName2 = new HashMap<>(RECORD_FIELDS_PERSON_2);

        recordWithoutName1.put(FIELD_NAME, null);
        recordWithoutName2.remove(FIELD_NAME);

        List<Record> records = new ArrayList<>();
        records.add(new MapRecord(schema, recordWithoutName1));
        records.add(new MapRecord(schema, recordWithoutName2));

        return new ListRecordSet(schema, records);
    }

    protected static List<RecordField> getSimpleRecordFields() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField(FIELD_NAME, RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField(FIELD_AGE, RecordFieldType.INT.getDataType()));
        fields.add(new RecordField(FIELD_COUNTRY, RecordFieldType.STRING.getDataType()));
        return fields;
    }

    protected static RecordSchema getSimpleSchema() {
        return new SimpleRecordSchema(getSimpleRecordFields(), SCHEMA_IDENTIFIER_PERSON);
    }

    /*
    Simple nested records
     */

    protected static RecordSet getNestedRecords() {
        RecordSchema innerSchema = getNestedSchema();

        final DataType recordType = RecordFieldType.RECORD.getRecordDataType(innerSchema);
        List<RecordField> fields = getSimpleRecordFields();
        fields.add(new RecordField(FIELD_ADDRESS, recordType));

        RecordSchema outerSchema = new SimpleRecordSchema(fields, SCHEMA_IDENTIFIER_PERSON);

        Record innerRecord1 = new MapRecord(innerSchema, RECORD_FIELDS_ADDRESS_1);
        Record outerRecord1 = new MapRecord(outerSchema, new HashMap<String,Object>(){{
            putAll(RECORD_FIELDS_PERSON_1);
            put(FIELD_ADDRESS, innerRecord1);
        }});

        Record innerRecord2 = new MapRecord(innerSchema, RECORD_FIELDS_ADDRESS_2);
        Record outerRecord2 = new MapRecord(outerSchema, new HashMap<String,Object>(){{
            putAll(RECORD_FIELDS_PERSON_2);
            put(FIELD_ADDRESS, innerRecord2);
        }});

        List<Record> records = new ArrayList<>();
        records.add(outerRecord1);
        records.add(outerRecord2);

        return new ListRecordSet(outerSchema, records);
    }

    protected static RecordSet getNestedRecordsWithNullValues() {
        RecordSchema innerSchema = getNestedSchema();

        final DataType recordType = RecordFieldType.RECORD.getRecordDataType(innerSchema);
        List<RecordField> fields = getSimpleRecordFields();
        fields.add(new RecordField(FIELD_ADDRESS, recordType));

        RecordSchema outerSchema = new SimpleRecordSchema(fields, SCHEMA_IDENTIFIER_PERSON);

        Map<String, Object> recordWithoutStreet1 = new HashMap<>(RECORD_FIELDS_ADDRESS_1);
        Map<String, Object> recordWithoutStreet2 = new HashMap<>(RECORD_FIELDS_ADDRESS_2);

        recordWithoutStreet1.put(FIELD_STREET, null);
        recordWithoutStreet2.remove(FIELD_STREET);

        Record innerRecord1 = new MapRecord(innerSchema, recordWithoutStreet1);
        Record outerRecord1 = new MapRecord(outerSchema, new HashMap<String,Object>(){{
            putAll(RECORD_FIELDS_PERSON_1);
            put(FIELD_ADDRESS, innerRecord1);
        }});

        Record innerRecord2 = new MapRecord(innerSchema, recordWithoutStreet2);
        Record outerRecord2 = new MapRecord(outerSchema, new HashMap<String,Object>(){{
            putAll(RECORD_FIELDS_PERSON_2);
            put(FIELD_ADDRESS, innerRecord2);
        }});

        List<Record> records = new ArrayList<>();
        records.add(outerRecord1);
        records.add(outerRecord2);

        return new ListRecordSet(outerSchema, records);
    }

    protected static RecordSet getNestedRecordsExclusivelyWithNullValues() {
        RecordSchema innerSchema = getNestedSchema();

        final DataType recordType = RecordFieldType.RECORD.getRecordDataType(innerSchema);
        List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField(FIELD_ADDRESS, recordType));
        fields.addAll(getSimpleRecordFields());

        RecordSchema outerSchema = new SimpleRecordSchema(fields, SCHEMA_IDENTIFIER_PERSON);

        Map<String, Object> recordWithoutStreet1 = new HashMap<>(RECORD_FIELDS_ADDRESS_1);
        Map<String, Object> recordWithoutStreet2 = new HashMap<>(RECORD_FIELDS_ADDRESS_2);

        recordWithoutStreet1.put(FIELD_STREET, null);
        recordWithoutStreet1.put(FIELD_CITY, null);
        recordWithoutStreet2.remove(FIELD_STREET);
        recordWithoutStreet2.remove(FIELD_CITY);

        Record innerRecord1 = new MapRecord(innerSchema, recordWithoutStreet1);
        Record outerRecord1 = new MapRecord(outerSchema, new HashMap<String,Object>(){{
            putAll(RECORD_FIELDS_PERSON_1);
            put(FIELD_ADDRESS, innerRecord1);
        }});

        Record innerRecord2 = new MapRecord(innerSchema, recordWithoutStreet2);
        Record outerRecord2 = new MapRecord(outerSchema, new HashMap<String,Object>(){{
            putAll(RECORD_FIELDS_PERSON_2);
            put(FIELD_ADDRESS, innerRecord2);
        }});

        List<Record> records = new ArrayList<>();
        records.add(outerRecord1);
        records.add(outerRecord2);

        return new ListRecordSet(outerSchema, records);
    }

    protected static List<RecordField> getNestedRecordFields() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField(FIELD_STREET, RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField(FIELD_CITY, RecordFieldType.STRING.getDataType()));
        return fields;
    }

    protected static RecordSchema getNestedSchema() {
        return new SimpleRecordSchema(getNestedRecordFields());
    }

    /*
    Arrays
     */

    protected static RecordSet getRecordWithSimpleArray() {
        Object[] children = {"Tom", "Anna", "Ben"};

        final DataType arrayType = RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType());

        final List<RecordField> fields = getSimpleRecordFields();
        fields.add(new RecordField(FIELD_CHILDREN, arrayType));

        Map<String,Object> recordFields1 = new HashMap<>();
        recordFields1.putAll(RECORD_FIELDS_PERSON_1);
        recordFields1.put(FIELD_CHILDREN, children);

        Map<String,Object> recordFields2 = new HashMap<>();
        recordFields2.putAll(RECORD_FIELDS_PERSON_1);
        recordFields2.put(FIELD_CHILDREN, children);

        RecordSchema schema = new SimpleRecordSchema(fields, SCHEMA_IDENTIFIER_PERSON);

        List<Record> records = new ArrayList<>();
        records.add(new MapRecord(schema, recordFields1));
        records.add(new MapRecord(schema, recordFields2));

        return new ListRecordSet(schema, records);
    }


}
