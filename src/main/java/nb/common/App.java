package nb.common;

import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.Slf4jReporter.LoggingLevel;

public final class App {

  private final static Logger log = LoggerFactory.getLogger(App.class);

  private static final MetricRegistry metrics;
  
  private App() {
  }

  static {
    metrics = new MetricRegistry();
  }

  public static MetricRegistry metrics() {
    return metrics;
  }

  public static void start(String metricsDomain) {
    if (Config.getSystemPropertyOrEnvVar("jmx.metrics", true)) {
      JmxReporter.forRegistry(metrics).inDomain(metricsDomain).build().start();
    }
    if (Config.getSystemPropertyOrEnvVar("log.metrics", false)) {
      Slf4jReporter.forRegistry(metrics).outputTo(LoggerFactory.getLogger(App.class))
                   .convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS)
                   .withLoggingLevel(LoggingLevel.INFO).build()
                   .start(Config.getSystemPropertyOrEnvVar("log.metrics.interval", 60), TimeUnit.SECONDS);
    }
  }

  public static boolean registerMBean(Object bean, String name) {
    try {
      ManagementFactory.getPlatformMBeanServer().registerMBean(bean, new ObjectName(name));
      return true;
    } catch (InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException
             | MalformedObjectNameException e) {
      log.error("Unable to register monitored topics MBean. They will be no exposed via JMX.", e);
      return false;
    }
  }
}
