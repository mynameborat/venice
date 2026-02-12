package com.linkedin.venice.helix;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.pushmonitor.PushControlSignal;
import com.linkedin.venice.pushmonitor.PushControlSignalType;
import java.io.IOException;
import org.testng.annotations.Test;


public class PushControlSignalJSONSerializerTest {
  private final PushControlSignalJSONSerializer serializer = new PushControlSignalJSONSerializer();

  @Test
  public void testSerializeDeserializeRoundTrip() throws IOException {
    PushControlSignal original = new PushControlSignal("testStore_v1");
    original.emitSignal(PushControlSignalType.BLOB_UPLOAD_COMPLETE, 1234567890L);

    byte[] bytes = serializer.serialize(original, "/test/path");
    PushControlSignal deserialized = serializer.deserialize(bytes, "/test/path");

    assertEquals(deserialized.getKafkaTopic(), "testStore_v1");
    assertTrue(deserialized.hasSignal(PushControlSignalType.BLOB_UPLOAD_COMPLETE));
    assertEquals(deserialized.getSignalTimestamp(PushControlSignalType.BLOB_UPLOAD_COMPLETE), 1234567890L);
    assertEquals(deserialized, original);
  }

  @Test
  public void testSerializeDeserializeEmptySignals() throws IOException {
    PushControlSignal original = new PushControlSignal("testStore_v2");

    byte[] bytes = serializer.serialize(original, "/test/path");
    PushControlSignal deserialized = serializer.deserialize(bytes, "/test/path");

    assertEquals(deserialized.getKafkaTopic(), "testStore_v2");
    assertFalse(deserialized.hasSignal(PushControlSignalType.BLOB_UPLOAD_COMPLETE));
    assertEquals(deserialized, original);
  }

  @Test
  public void testDeserializeIgnoresUnknownFields() throws IOException {
    // JSON with an extra field that doesn't exist in PushControlSignal
    String json = "{\"kafkaTopic\":\"testStore_v3\",\"signals\":{},\"unknownField\":\"someValue\"}";
    byte[] bytes = json.getBytes();

    PushControlSignal deserialized = serializer.deserialize(bytes, "/test/path");

    assertEquals(deserialized.getKafkaTopic(), "testStore_v3");
    assertFalse(deserialized.hasSignal(PushControlSignalType.BLOB_UPLOAD_COMPLETE));
  }
}
