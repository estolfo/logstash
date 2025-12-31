# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

require "logstash/instrument/periodic_poller/base"

java_import 'org.logstash.instrument.metrics.otel.OtelMetricsService'
java_import 'io.opentelemetry.api.common.Attributes'
java_import 'io.opentelemetry.api.common.AttributeKey'

module LogStash module Instrument module PeriodicPoller
  # Exports Logstash metrics to an OpenTelemetry-compatible backend via OTLP.
  #
  # This poller:
  # - Reads metrics from Logstash's MetricStore
  # - Registers them as OTel instruments (counters and gauges)
  # - The OTel SDK handles periodic export via the configured OTLP endpoint
  #
  # Configuration in logstash.yml:
  #   otel.metrics.enabled: true
  #   otel.metrics.endpoint: "http://localhost:4317"
  #   otel.metrics.interval: 10
  #   otel.metrics.protocol: "grpc"
  #   otel.resource.attributes: "environment=production,cluster=us-west"
  #
  class Otel < Base
    def initialize(metric, agent, settings)
      # Use a longer polling interval since OTel SDK handles its own export timing
      # We just need to periodically update counter deltas
      super(metric, { :polling_interval => settings.get("otel.metrics.interval") })

      @agent = agent
      @settings = settings
      @metric_store = agent.metric.collector

      # Track previous counter values to compute deltas
      @last_values = {}

      # Initialize the OTel service
      @otel_service = OtelMetricsService.new(
        settings.get("otel.metrics.endpoint"),
        agent.id,
        agent.name,
        settings.get("otel.metrics.interval"),
        settings.get("otel.metrics.protocol"),
        settings.get("otel.resource.attributes")
      )

      # Register all metrics
      register_gauges
      register_counters

      logger.info("OpenTelemetry metrics poller initialized",
                  :endpoint => settings.get("otel.metrics.endpoint"),
                  :interval => settings.get("otel.metrics.interval"))
    end

    def collect
      # For counters, we need to compute and send deltas
      # Gauges are handled via callbacks in the OTel SDK
      collect_counter_deltas
    rescue => e
      logger.error("Error collecting OTel metrics", :exception => e.message, :backtrace => e.backtrace)
    end

    def stop
      logger.info("Stopping OpenTelemetry metrics poller")
      super
      @otel_service.flush
      @otel_service.shutdown
    end

    private

    # Register observable gauges - these use callbacks that the OTel SDK invokes
    def register_gauges
      # JVM Memory gauges
      register_gauge("logstash.jvm.memory.heap.used", "JVM heap memory in use", "By") do
        get_metric_value(:jvm, :memory, :heap, :used_in_bytes)
      end

      register_gauge("logstash.jvm.memory.heap.max", "Maximum JVM heap memory", "By") do
        get_metric_value(:jvm, :memory, :heap, :max_in_bytes)
      end

      register_gauge("logstash.jvm.memory.heap.committed", "Committed JVM heap memory", "By") do
        get_metric_value(:jvm, :memory, :heap, :committed_in_bytes)
      end

      register_gauge("logstash.jvm.memory.non_heap.used", "JVM non-heap memory in use", "By") do
        get_metric_value(:jvm, :memory, :non_heap, :used_in_bytes)
      end

      # JVM Thread gauges
      register_gauge("logstash.jvm.threads.count", "Current JVM thread count", "{threads}") do
        get_metric_value(:jvm, :threads, :count)
      end

      register_gauge("logstash.jvm.threads.peak_count", "Peak JVM thread count", "{threads}") do
        get_metric_value(:jvm, :threads, :peak_count)
      end

      # Process gauges
      register_gauge("logstash.process.open_file_descriptors", "Open file descriptors", "{descriptors}") do
        get_metric_value(:jvm, :process, :open_file_descriptors)
      end

      register_gauge("logstash.process.cpu.percent", "Process CPU usage percentage", "%") do
        get_metric_value(:jvm, :process, :cpu, :percent)
      end

      # Queue gauge (total across all pipelines)
      register_gauge("logstash.queue.events", "Total events in queues", "{events}") do
        get_total_queue_events
      end

      # Per-pipeline gauges
      register_pipeline_gauges
    end

    def register_pipeline_gauges
      # These will be registered for each running pipeline
      # Note: In a real implementation, you'd want to handle dynamic pipeline add/remove
      @agent.pipelines_registry.running_pipelines.each do |pipeline_id, _pipeline|
        attrs = create_pipeline_attributes(pipeline_id)

        register_gauge_with_attrs(
          "logstash.pipeline.events.queue_size",
          "Events in pipeline queue",
          "{events}",
          attrs
        ) do
          get_pipeline_metric_value(pipeline_id, :queue, :events)
        end
      end
    end

    # Register counters - we track these and send deltas
    def register_counters
      # Create counter instruments
      @events_in_counter = @otel_service.getOrCreateCounter(
        "logstash.events.in",
        "Total events received",
        "{events}"
      )

      @events_out_counter = @otel_service.getOrCreateCounter(
        "logstash.events.out",
        "Total events output",
        "{events}"
      )

      @events_filtered_counter = @otel_service.getOrCreateCounter(
        "logstash.events.filtered",
        "Total events filtered",
        "{events}"
      )
    end

    # Collect counter deltas and increment OTel counters
    def collect_counter_deltas
      snapshot = @metric_store.snapshot_metric
      store = snapshot.metric_store

      begin
        events = store.get_shallow(:stats, :events)

        # Calculate and send deltas for each counter
        send_counter_delta(@events_in_counter, :events_in, events[:in]&.value || 0)
        send_counter_delta(@events_out_counter, :events_out, events[:out]&.value || 0)
        send_counter_delta(@events_filtered_counter, :events_filtered, events[:filtered]&.value || 0)

        # Per-pipeline counters
        collect_pipeline_counter_deltas(store)
      rescue LogStash::Instrument::MetricStore::MetricNotFound => e
        logger.debug("Metrics not yet available", :error => e.message)
      end
    end

    def collect_pipeline_counter_deltas(store)
      begin
        pipelines = store.get_shallow(:stats, :pipelines)
        pipelines.each do |pipeline_id, _data|
          attrs = create_pipeline_attributes(pipeline_id)

          pipeline_events = store.get_shallow(:stats, :pipelines, pipeline_id, :events) rescue nil
          next unless pipeline_events

          # Get or create per-pipeline counters
          pipeline_in_counter = @otel_service.getOrCreateCounter(
            "logstash.pipeline.events.in",
            "Events received by pipeline",
            "{events}"
          )

          pipeline_out_counter = @otel_service.getOrCreateCounter(
            "logstash.pipeline.events.out",
            "Events output by pipeline",
            "{events}"
          )

          # Calculate and send deltas
          current_in = pipeline_events[:in]&.value || 0
          current_out = pipeline_events[:out]&.value || 0

          key_in = "pipeline_#{pipeline_id}_in".to_sym
          key_out = "pipeline_#{pipeline_id}_out".to_sym

          delta_in = current_in - (@last_values[key_in] || 0)
          delta_out = current_out - (@last_values[key_out] || 0)

          pipeline_in_counter.add(delta_in, attrs) if delta_in > 0
          pipeline_out_counter.add(delta_out, attrs) if delta_out > 0

          @last_values[key_in] = current_in
          @last_values[key_out] = current_out
        end
      rescue LogStash::Instrument::MetricStore::MetricNotFound
        # Pipelines not yet available
      end
    end

    def send_counter_delta(counter, key, current_value)
      delta = current_value - (@last_values[key] || 0)
      counter.add(delta, Attributes.empty) if delta > 0
      @last_values[key] = current_value
    end

    # Helper to register a gauge with a Ruby block as callback
    def register_gauge(name, description, unit, &block)
      register_gauge_with_attrs(name, description, unit, Attributes.empty, &block)
    end

    def register_gauge_with_attrs(name, description, unit, attributes, &block)
      # Wrap Ruby block in a Java Supplier
      supplier = -> {
        begin
          value = block.call
          value.nil? ? nil : value.to_java(:long)
        rescue => e
          logger.debug("Error getting gauge value for #{name}", :error => e.message)
          nil
        end
      }

      @otel_service.registerGauge(name, description, unit, supplier, attributes)
    end

    # Helper to get metric values from the store
    def get_metric_value(*path)
      snapshot = @metric_store.snapshot_metric
      store = snapshot.metric_store

      result = store.get_shallow(*path)
      result.is_a?(Hash) ? nil : result&.value
    rescue LogStash::Instrument::MetricStore::MetricNotFound
      nil
    end

    def get_pipeline_metric_value(pipeline_id, *path)
      full_path = [:stats, :pipelines, pipeline_id.to_sym] + path
      get_metric_value(*full_path)
    end

    def get_total_queue_events
      total = 0
      @agent.pipelines_registry.running_pipelines.each do |pipeline_id, pipeline|
        next if pipeline.system?
        queue_events = get_pipeline_metric_value(pipeline_id, :queue, :events)
        total += queue_events if queue_events
      end
      total
    end

    def create_pipeline_attributes(pipeline_id)
      Attributes.of(
        AttributeKey.stringKey("pipeline.id"), pipeline_id.to_s
      )
    end
  end
end; end; end
