package io.ray.streaming.runtime.config.global;

import io.ray.streaming.common.config.Config;
import io.ray.streaming.runtime.config.converter.MetricEnableConverter;
import io.ray.streaming.runtime.config.converter.MetricReporterConverter;
import org.aeonbits.owner.Mutable;

public interface MetricConfig extends Config, Mutable {

  // ====================================================================================
  // Metrics common configuration
  // ====================================================================================


  String METRIC_ENABLE = "streaming.metric.enable";
  String METRIC_TYPE = "streaming.metric.type";
  String METRICS_SCOPE_JOB_MASTER = "streaming.metrics.jobmaster";
  String METRICS_SCOPE_JOB_WORKER = "streaming.metrics.jobworker";
  String METRICS_SCOPE_BROKER = "streaming.metrics.broker";
  String METRICS_SCOPE_MASTER_REMOTE_CALL = "streaming.metrics.jobmaster.remotecall";
  String METRICS_SCOPE_WORKER_REMOTE_CALL = "streaming metrics.jobworker.remotecall";
  String METRICS_SCOPE_QUEUE = "streaming.metrics.queue";

  String METRICS_KMONITOR_TENANT_NAME = "streaming.metrics.kmonitor.tenant.name";
  String METRICS_KMONITOR_SERVICE_NAME = "streaming.metrics.kmonitor.service.name";
  String METRICS_REPORTERS = "streaming.metrics.reporters";
  String METRICS_REPORTER_DEFAULT_INTERVAL = "streaming.metrics.reporter.default.interval";
  String METRICS_REPORTER_GLOBAL_SCOPE_DELIMITER = "streaming.metrics.reporter.global.scope.delimiter";

  String METRICS_REPORTER_INTERVAL_SEPARATOR = ",";
  String METRICS_REPORTER_EXTERNAL_SEPARATOR = ";";
  String METRICS_REPORTERS_DEFAULT_VALUE = "ray_metric,1,.";

  String METRIC_WORKER_SAMPLE_FREQUENCY = "streaming.metric.worker.sample.frequency";
  String METRIC_TRUE_RATE_AGGREGATOR_INTERVAL_SEC = "streaming.metric.true.rate.aggregator.interval.sec";
  String METRIC_TRUE_RATE_ENABLE = "streaming.metric.true.rate.enable";

  // ====================================================================================
  // MetricFetcher configuration
  // ====================================================================================

  String METRICS_FETCHER_UPDATE_INTERVAL = "streaming.metrics.fetcher.update.interval";
  String METRICS_FETCHER_TIMEOUT = "streaming.metrics.fetcher.timeout";
  String METRICS_FETCHER_QUEUE_ENABLE = "streaming.metrics.fetcher.queue.enable";

  // ====================================================================================
  // MetricProvider configuration
  // ====================================================================================

  String METRICS_PROVIDER_SERVICE_ENABLE = "streaming.metrics.provider.enable";
  String METRICS_PROVIDER_UPDATE_INTERVAL = "streaming.metrics.provider.update.interval";

  // ====================================================================================
  // External - KMonitor configuration
  // ====================================================================================
  String EXTERNAL_METRICS_QUERY_KMONITOR_ENABLE = "streaming.metrics.query.kmonitor.enable";
  String EXTERNAL_METRICS_QUERY_KMONITOR_URL = "streaming.metrics.query.kmonitor.url";
  String EXTERNAL_METRICS_QUERY_KMONITOR_CONTENT = "streaming.metrics.query.kmonitor.content";
  String EXTERNAL_METRICS_QUERY_KMONITOR_TIME_RANGE = "streaming.metrics.query.kmonitor.time-range";

  String METRICS_WORKER_LATENCY_THRESHOLD_MS = "streaming.metric.worker.latency.threshold.ms";

  // ====================================================================================
  // External - TSDB configuration
  // ====================================================================================
  String METRICS_TSDB_SERVICE_NAME = "streaming.metrics.tsdb.service.name";


  /**
   * metric fetcher update interval in millisecond.
   */
  @Key(METRICS_FETCHER_UPDATE_INTERVAL)
  @DefaultValue("10000")
  Long metricsFetcherUpdateInterval();

  /**
   * metric fetcher ray call worker timeout in millisecond.
   */
  @Key(METRICS_FETCHER_TIMEOUT)
  @DefaultValue("6000")
  Integer metricsFetcherTimeout();

  /**
   * Enable queue metrics fetcher or not.
   */
  @Key(METRICS_FETCHER_QUEUE_ENABLE)
  @DefaultValue("true")
  boolean metricsFetcherQueueEnable();

  /**
   * Enable metrics provider service or not.
   */
  @Key(METRICS_PROVIDER_SERVICE_ENABLE)
  @DefaultValue("true")
  boolean metricsProviderServiceEnable();

  /**
   * metric provider update interval in millisecond.
   */
  @Key(METRICS_PROVIDER_UPDATE_INTERVAL)
  @DefaultValue("10000")
  Long metricsProviderUpdateInterval();

  @Key(METRICS_SCOPE_JOB_MASTER)
  @DefaultValue("<host>.jobmaster.<job_name>.<pid>")
  String metricsScopeJobMaster();

  @Key(METRICS_SCOPE_JOB_WORKER)
  @DefaultValue("<host>.jobworker.<job_name>.<op_name>.<worker_id>.<worker_name>.<pid>")
  String metricsScopeJobWorker();

  @Key(METRICS_SCOPE_BROKER)
  @DefaultValue("<host>.broker.<job_name>.<pid>")
  String metricScopeBroker();

  @Key(METRICS_SCOPE_MASTER_REMOTE_CALL)
  @DefaultValue("<host>.jobmaster.<job_name>.<pid>.<remote_op_name>.<remote_op_index>.<remote_worker_id>")
  String metricsScopeMasterRemoteCall();

  /**
   * Last three scopes are remote worker's
   */
  @Key(METRICS_SCOPE_WORKER_REMOTE_CALL)
  @DefaultValue("<host>.jobworker.<job_name>.<op_name>.<worker_id>.<pid>.<remote_op_name>.<remote_worker_id>.<remote_worker_name>")
  String metricsScopeWorkerRemoteCall();

  @Key(METRICS_SCOPE_QUEUE)
  @DefaultValue("<host>.jobworker.<job_name>.<op_name>.<worker_id>.<worker_name>.<pid>.<queue_id>")
  String metricsScopeQueue();

  @Key(METRICS_KMONITOR_TENANT_NAME)
  @DefaultValue("")
  String metricsKmonitorTenantName();

  @Key(METRICS_KMONITOR_SERVICE_NAME)
  @DefaultValue("")
  String metricsKmonitorServiceName();

  @Key(METRICS_TSDB_SERVICE_NAME)
  @DefaultValue("alipay_stm")
  String metricsTsdbServiceName();

  /**
   * Metric reporter format is:
   * <pre>
   * reporter_name,interval,delimiter;reporter_name,interval,delimiter
   * </pre>
   * <p>
   * interval time unit is second
   * </p>
   * you can only assign reporter_name,interval and use global delimiter or only reporter_name and
   * use default interval and default delimiter. Do not report to any reporter if you config this
   * configuration empty.
   */
  @Key(METRICS_REPORTERS)
  @DefaultValue(METRICS_REPORTERS_DEFAULT_VALUE)
  @ConverterClass(MetricReporterConverter.class)
  String metricsReporters();

  @Key(METRICS_REPORTER_DEFAULT_INTERVAL)
  @DefaultValue("3")
  int metricsReporterDefaultInterval();

  @Key(METRICS_REPORTER_GLOBAL_SCOPE_DELIMITER)
  @DefaultValue(".")
  String metricsReporterGlobalScopeDelimiter();

  @Key(EXTERNAL_METRICS_QUERY_KMONITOR_ENABLE)
  @DefaultValue("false")
  boolean kmonitorQueryEnable();

  /**
   * KMonitor query url.
   *
   * @return query url
   */
  @Key(EXTERNAL_METRICS_QUERY_KMONITOR_URL)
  @DefaultValue("http://kmonitor-query.alipay.com/api/query")
  String kmonitorQueryUrl();

  /**
   * KMonitor query time range. unit: minute. defalut: 5mins-ago
   *
   * @return range of time
   */
  @Key(EXTERNAL_METRICS_QUERY_KMONITOR_TIME_RANGE)
  @DefaultValue("5")
  int kmonitorQueryRangeFromNow();

  @DefaultValue(value = "true")
  @ConverterClass(MetricEnableConverter.class)
  @Key(value = METRIC_ENABLE)
  boolean metricEnable();

  @DefaultValue(value = "ray_metric")
  @Key(value = METRIC_TYPE)
  String metricType();

  /**
   * worker metric (MetricNames.WK_CONSUME_MSG and MetricNames.WK_PRODUCE_MSG) sample frequency
   */
  @DefaultValue(value = "1000")
  @Key(value = METRIC_WORKER_SAMPLE_FREQUENCY)
  long metricWorkerSampleFrequency();

  /**
   * Aggregator interval for compute True Processing Rate and True Output Rate. Unit: second.
   * Default: 120s
   *
   * @return the agg window length
   */
  @DefaultValue(value = "120")
  @Key(value = METRIC_TRUE_RATE_AGGREGATOR_INTERVAL_SEC)
  long trueRateAggregatorInterval();

  @DefaultValue(value = "false")
  @Key(value = METRIC_TRUE_RATE_ENABLE)
  boolean trueRateMetricEnable();

  @DefaultValue(value = "5")
  @Key(value = METRICS_WORKER_LATENCY_THRESHOLD_MS)
  long workerLatencyThresholdMs();
}
