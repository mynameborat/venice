package com.linkedin.venice.helix;

import com.linkedin.venice.pushmonitor.PushControlSignal;


/**
 * Serializer used to convert the data between {@link PushControlSignal} and JSON.
 */
public class PushControlSignalJSONSerializer extends VeniceJsonSerializer<PushControlSignal> {
  public PushControlSignalJSONSerializer() {
    super(PushControlSignal.class);
  }
}
