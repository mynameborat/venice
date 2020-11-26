/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.linkedin.venice.systemstore.schemas;

@SuppressWarnings("all")
/** A composite key that divides the key space to retrieve different types of metadata for a Venice store. StoreAttributes and TargetVersionStates are global properties therefore shouldn't have cluster and fabric names in the keyStrings array. In contrast, CurrentStoreStates and CurrentVersionStates are cluster and fabric specific states that's identified with corresponding cluster and fabric names in the keyStrings array. */
public class StoreMetaKey extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"StoreMetaKey\",\"namespace\":\"com.linkedin.venice.systemstore.schemas\",\"fields\":[{\"name\":\"keyStrings\",\"type\":{\"type\":\"array\",\"items\":\"string\"},\"doc\":\"An array of Strings to identify the record and create different key spaces within the same metadataType (e.g. used to support manifesting information for many fabrics and a store that exists in many clusters). The expected indexes and corresponding entries are as follows: 0 => Store name, 1 => Cluster name, 2 => Fabric name\"},{\"name\":\"metadataType\",\"type\":\"int\",\"doc\":\"An integer (mapped to Enums) specifying the metadata type of the record. 1 => StoreProperties, 2 => StoreKeySchemas, 3 => StoreValueSchemas, 4 => StoreReplicaStatuses.\"}]}");
  /** An array of Strings to identify the record and create different key spaces within the same metadataType (e.g. used to support manifesting information for many fabrics and a store that exists in many clusters). The expected indexes and corresponding entries are as follows: 0 => Store name, 1 => Cluster name, 2 => Fabric name */
  public java.util.List<java.lang.CharSequence> keyStrings;
  /** An integer (mapped to Enums) specifying the metadata type of the record. 1 => StoreProperties, 2 => StoreKeySchemas, 3 => StoreValueSchemas, 4 => StoreReplicaStatuses. */
  public int metadataType;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return keyStrings;
    case 1: return metadataType;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: keyStrings = (java.util.List<java.lang.CharSequence>)value$; break;
    case 1: metadataType = (java.lang.Integer)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
