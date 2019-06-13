package nb.kafka.operator.util;

import java.util.Collection;
import java.util.Optional;

import io.micrometer.core.instrument.Meter;

public final class MeterUtils {
  private MeterUtils() {
  }

  public static <T extends Meter> Optional<T> filterMeterByTags(Collection<T> meters, String... tags) {
    return meters.stream().filter((meter) -> {
      Meter.Id id = meter.getId();
      for (int i = 0; i < tags.length; i += 2) {
        String value = id.getTag(tags[i]);
        if (!tags[i + 1].equalsIgnoreCase(value)) {
          return false;
        }
      }
      return true;
    }).findFirst();
  }
}
