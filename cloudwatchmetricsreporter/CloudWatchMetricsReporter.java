package custom;

import java.util.concurrent.ScheduledExecutorService; 
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;

import org.apache.hudi.metrics.userdefined.AbstractUserDefinedMetricsReporter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.hudi.com.codahale.metrics.MetricRegistry;

import java.util.Properties;
import java.io.Closeable;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.PutMetricDataResult;
import com.amazonaws.services.cloudwatch.model.StandardUnit;

public class CloudWatchMetricsReporter 
    extends AbstractUserDefinedMetricsReporter {
  private static final Logger log = LogManager.getLogger("custom.hudi");
  private final AmazonCloudWatch cw =
    AmazonCloudWatchClientBuilder.defaultClient();

  private ScheduledExecutorService exec = Executors.newScheduledThreadPool(1, r -> {
      Thread t = Executors.defaultThreadFactory().newThread(r);
      t.setDaemon(true);
      return t;
  });

  public CloudWatchMetricsReporter(Properties props, MetricRegistry registry) {
    super(props, registry);
  }

  public void pushCWMetric(AmazonCloudWatch client, String key, Double metric_value){

    String[] parts = key.split("\\.",2);
    String table_name=parts[0];
    String metric_key=parts[1];
    log.info("CloudWatchMetricsReporter table_name : "+table_name);
    log.info("CloudWatchMetricsReporter metric_key : "+metric_key);

    Dimension dimension = new Dimension()
    .withName("TABLE")
    .withValue(table_name);

    MetricDatum datum = new MetricDatum()
        .withMetricName(metric_key)
        .withUnit(StandardUnit.None)
        .withValue(metric_value)
        .withDimensions(dimension);

    PutMetricDataRequest request = new PutMetricDataRequest()
        .withNamespace("HUDI")
        .withMetricData(datum);

    PutMetricDataResult response = cw.putMetricData(request);

    log.info("CloudWatchMetricsReporter PutMetricDataResult : "+response);
      }

  @Override
  public void start() {
    exec.schedule(this::report, 10, TimeUnit.SECONDS);
  }

  @Override
  public void report() {
    this.getRegistry().getGauges().forEach((key, value) ->  {
      log.info("CloudWatchMetricsReporter - key: " + key + " value: " + value.getValue().toString());
      pushCWMetric(cw, key, Double.parseDouble(value.getValue().toString()));
    });
  }

  @Override
  public Closeable getReporter() {
    return null;
  }

  @Override
  public void stop() {
    exec.shutdown();
  }
}
