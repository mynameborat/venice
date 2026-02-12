package com.linkedin.venice.pushmonitor;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.io.IOException;
import java.util.Map;
import org.testng.annotations.Test;


public class PushControlSignalTest {
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();

  @Test
  public void testCreatePushControlSignal() {
    PushControlSignal signal = new PushControlSignal("testStore_v1");
    assertEquals(signal.getKafkaTopic(), "testStore_v1");
    assertTrue(signal.getSignals().isEmpty());
  }

  @Test
  public void testEmitAndQuerySignal() {
    PushControlSignal signal = new PushControlSignal("testStore_v1");

    assertFalse(signal.hasSignal(PushControlSignalType.BLOB_UPLOAD_COMPLETE));
    assertEquals(signal.getSignalTimestamp(PushControlSignalType.BLOB_UPLOAD_COMPLETE), -1L);

    long timestamp = 1234567890L;
    signal.emitSignal(PushControlSignalType.BLOB_UPLOAD_COMPLETE, timestamp);

    assertTrue(signal.hasSignal(PushControlSignalType.BLOB_UPLOAD_COMPLETE));
    assertEquals(signal.getSignalTimestamp(PushControlSignalType.BLOB_UPLOAD_COMPLETE), timestamp);
  }

  @Test
  public void testEmitSignalWithDefaultTimestamp() {
    PushControlSignal signal = new PushControlSignal("testStore_v1");
    long before = System.currentTimeMillis();
    signal.emitSignal(PushControlSignalType.BLOB_UPLOAD_COMPLETE);
    long after = System.currentTimeMillis();

    long ts = signal.getSignalTimestamp(PushControlSignalType.BLOB_UPLOAD_COMPLETE);
    assertTrue(ts >= before && ts <= after);
  }

  @Test
  public void testSignalsMapIsUnmodifiable() {
    PushControlSignal signal = new PushControlSignal("testStore_v1");
    signal.emitSignal(PushControlSignalType.BLOB_UPLOAD_COMPLETE, 100L);

    Map<PushControlSignalType, Long> signals = signal.getSignals();
    try {
      signals.put(PushControlSignalType.BLOB_UPLOAD_COMPLETE, 200L);
      throw new AssertionError("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // expected
    }
  }

  @Test
  public void testEqualsAndHashCode() {
    PushControlSignal a = new PushControlSignal("testStore_v1");
    PushControlSignal b = new PushControlSignal("testStore_v1");
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());

    a.emitSignal(PushControlSignalType.BLOB_UPLOAD_COMPLETE, 100L);
    assertNotEquals(a, b);

    b.emitSignal(PushControlSignalType.BLOB_UPLOAD_COMPLETE, 100L);
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  public void testJsonSerializationRoundTrip() throws IOException {
    PushControlSignal original = new PushControlSignal("testStore_v1");
    original.emitSignal(PushControlSignalType.BLOB_UPLOAD_COMPLETE, 1234567890L);

    String json = OBJECT_MAPPER.writeValueAsString(original);
    PushControlSignal deserialized = OBJECT_MAPPER.readValue(json, PushControlSignal.class);

    assertEquals(deserialized, original);
    assertEquals(deserialized.getKafkaTopic(), "testStore_v1");
    assertTrue(deserialized.hasSignal(PushControlSignalType.BLOB_UPLOAD_COMPLETE));
    assertEquals(deserialized.getSignalTimestamp(PushControlSignalType.BLOB_UPLOAD_COMPLETE), 1234567890L);
  }

  @Test
  public void testJsonConstructorWithNullSignals() {
    PushControlSignal signal = new PushControlSignal("testStore_v1", null);
    assertEquals(signal.getKafkaTopic(), "testStore_v1");
    assertTrue(signal.getSignals().isEmpty());
  }

  @Test
  public void testToString() {
    PushControlSignal signal = new PushControlSignal("testStore_v1");
    String str = signal.toString();
    assertTrue(str.contains("testStore_v1"));
    assertTrue(str.contains("PushControlSignal"));
  }
}
