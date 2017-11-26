package nb.common;

import java.lang.management.ManagementFactory;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;

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
    JmxReporter.forRegistry(metrics()).inDomain(metricsDomain).build().start();
  }

  public static boolean registerMBean(Object bean, String name) {
    try {
      ManagementFactory.getPlatformMBeanServer().registerMBean(bean, new ObjectName(name));
      return true;
    } catch (InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException
             | MalformedObjectNameException e) {
      log.error("Unable to register monitored topics MBean. They will not be exposed via JMX.", e);
      return false;
    }
  }
}
