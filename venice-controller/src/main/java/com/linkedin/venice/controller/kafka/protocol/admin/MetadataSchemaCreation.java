/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.linkedin.venice.controller.kafka.protocol.admin;

@SuppressWarnings("all")
public class MetadataSchemaCreation extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"MetadataSchemaCreation\",\"namespace\":\"com.linkedin.venice.controller.kafka.protocol.admin\",\"fields\":[{\"name\":\"clusterName\",\"type\":\"string\"},{\"name\":\"storeName\",\"type\":\"string\"},{\"name\":\"valueSchemaId\",\"type\":\"int\"},{\"name\":\"metadataSchema\",\"type\":{\"type\":\"record\",\"name\":\"SchemaMeta\",\"fields\":[{\"name\":\"schemaType\",\"type\":\"int\",\"doc\":\"0 => Avro-1.4, and we can add more if necessary\"},{\"name\":\"definition\",\"type\":\"string\"}]}},{\"name\":\"timestampMetadataVersionId\",\"type\":\"int\",\"default\":-1}]}");
  public java.lang.CharSequence clusterName;
  public java.lang.CharSequence storeName;
  public int valueSchemaId;
  public com.linkedin.venice.controller.kafka.protocol.admin.SchemaMeta metadataSchema;
  public int timestampMetadataVersionId;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return clusterName;
    case 1: return storeName;
    case 2: return valueSchemaId;
    case 3: return metadataSchema;
    case 4: return timestampMetadataVersionId;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: clusterName = (java.lang.CharSequence)value$; break;
    case 1: storeName = (java.lang.CharSequence)value$; break;
    case 2: valueSchemaId = (java.lang.Integer)value$; break;
    case 3: metadataSchema = (com.linkedin.venice.controller.kafka.protocol.admin.SchemaMeta)value$; break;
    case 4: timestampMetadataVersionId = (java.lang.Integer)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}