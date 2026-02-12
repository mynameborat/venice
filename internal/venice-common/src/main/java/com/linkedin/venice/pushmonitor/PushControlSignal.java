package com.linkedin.venice.pushmonitor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;


/**
 * Represents a set of controller-emitted control signals for a single push (version topic).
 *
 * <p>Stored in ZK at a dedicated path (e.g. {@code /PushControlSignals/$topic}) and watched
 * by servers. This complements {@link OfflinePushStatus}, which carries server→controller
 * replica status updates. {@code PushControlSignal} carries controller→server notifications.
 *
 * <p>Each signal type is stored with a timestamp indicating when it was emitted. New signal
 * types can be added to {@link PushControlSignalType} as the system evolves.
 */
public class PushControlSignal {
  private final String kafkaTopic;
  private final Map<PushControlSignalType, Long> signals;

  public PushControlSignal(String kafkaTopic) {
    this.kafkaTopic = kafkaTopic;
    this.signals = new EnumMap<>(PushControlSignalType.class);
  }

  @JsonCreator
  public PushControlSignal(
      @JsonProperty("kafkaTopic") String kafkaTopic,
      @JsonProperty("signals") Map<PushControlSignalType, Long> signals) {
    this.kafkaTopic = kafkaTopic;
    this.signals = signals == null ? new EnumMap<>(PushControlSignalType.class) : new EnumMap<>(signals);
  }

  public String getKafkaTopic() {
    return kafkaTopic;
  }

  /**
   * @return an unmodifiable view of all active signals and their emission timestamps.
   */
  public Map<PushControlSignalType, Long> getSignals() {
    return Collections.unmodifiableMap(signals);
  }

  /**
   * Emit a signal with the current wall-clock time.
   */
  public void emitSignal(PushControlSignalType type) {
    emitSignal(type, System.currentTimeMillis());
  }

  /**
   * Emit a signal with an explicit timestamp.
   */
  public void emitSignal(PushControlSignalType type, long timestampMs) {
    signals.put(type, timestampMs);
  }

  /**
   * @return true if the given signal has been emitted.
   */
  public boolean hasSignal(PushControlSignalType type) {
    return signals.containsKey(type);
  }

  /**
   * @return the timestamp (epoch millis) when the signal was emitted, or -1 if not present.
   */
  public long getSignalTimestamp(PushControlSignalType type) {
    return signals.getOrDefault(type, -1L);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PushControlSignal that = (PushControlSignal) o;
    return kafkaTopic.equals(that.kafkaTopic) && signals.equals(that.signals);
  }

  @Override
  public int hashCode() {
    return Objects.hash(kafkaTopic, signals);
  }

  @Override
  public String toString() {
    return "PushControlSignal{kafkaTopic='" + kafkaTopic + "', signals=" + signals + "}";
  }
}
