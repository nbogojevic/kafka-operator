package nb.kafka.operator.util;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;

/**
 * Centralized manager for tracking all meter instances.
 */
public class MeterManager {
  private final MeterRegistry registry;
  private final Map<Meter.Id, Meter> meters = new ConcurrentHashMap<>();

  private static MeterManager defaultMeterManager;
  public static final MeterManager defaultMeterManager() {
    if (defaultMeterManager == null) {
      defaultMeterManager = new MeterManager(Metrics.globalRegistry);
    }
    return defaultMeterManager;
  }

  public MeterManager(MeterRegistry registry) {
    this.registry = Objects.requireNonNull(registry);
  }

  public <T> Gauge register(Gauge.Builder<T> builder) {
    return register(builder.register(registry));
  }

  public Counter register(Counter.Builder builder) {
    return register(builder.register(registry));
  }

  public Timer register(Timer.Builder builder) {
    return register(builder.register(registry));
  }

  public DistributionSummary register(DistributionSummary.Builder builder) {
    return register(builder.register(registry));
  }

  private <T extends Meter> T register(T meter) {
    meters.put(meter.getId(), meter);
    return meter;
  }

  public Meter close(Meter.Id id) {
    Meter meter = meters.remove(id);
    if (meter != null) {
      meter.close();
      registry.remove(meter);
    }
    return meter;
  }

  public void close(Meter meter) {
    close(meter.getId());
  }

  public Meter getMeter(Meter.Id id) {
    return meters.get(id);
  }

  public MeterRegistry getRegistry() {
    return registry;
  }

  public Set<Meter.Id> getMeterIds() {
    return meters.keySet();
  }

  public void close() {
    meters.values().forEach(meter -> {
      meter.close();
      registry.remove(meter);
    });
    meters.clear();
  }
}
