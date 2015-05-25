/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.oozie.service;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ganglia.GangliaReporter;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import info.ganglia.gmetric4j.gmetric.GMetric;

public class PopulatorMetricsReportingManagerService {

    private static final String GRAPHITE="graphite";
    private static final String GANGLIA="ganglia";
    private static final String METRICS_SERVER_NAME ="METRICS_SERVER_NAME";
    private String METRICS_HOST;
    private String METRICS_PREFIX;

    private long METRICS_REPORT_INTERVAL_SEC;
    private int METRICS_PORT;

    private GraphiteReporter graphiteReporter = null;
    private GangliaReporter gangliaReporter = null;
    private long metricsReportIntervalSec;

    public PopulatorMetricsReportingManagerService() {
        METRICS_HOST = ConfigurationService.get("METRICS_HOST");
        METRICS_PREFIX = ConfigurationService.get("METRICS_PREFIX");
        METRICS_REPORT_INTERVAL_SEC = ConfigurationService.getLong("METRICS_REPORT_INTERVAL_SEC");
        METRICS_PORT = ConfigurationService.getInt("METRICS_PORT");
    }

    public void init(MetricRegistry metricRegistry) {

        if(ConfigurationService.get(METRICS_SERVER_NAME).equals(GRAPHITE)) {
            Graphite graphite = new Graphite(new InetSocketAddress(METRICS_HOST, METRICS_PORT));
            graphiteReporter = GraphiteReporter.forRegistry(metricRegistry).prefixedWith(METRICS_PREFIX)
                    .convertDurationsTo(TimeUnit.SECONDS).filter(MetricFilter.ALL).build(graphite);
        }

        if(ConfigurationService.get(METRICS_SERVER_NAME).equals(GANGLIA)) {
            GMetric ganglia = null;
            try {
                ganglia = new GMetric(METRICS_HOST, METRICS_PORT, GMetric.UDPAddressingMode.MULTICAST, 1);
            } catch (IOException e) {
                e.printStackTrace();
            }
            gangliaReporter = GangliaReporter.forRegistry(metricRegistry)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build(ganglia);
        }

        metricsReportIntervalSec = METRICS_REPORT_INTERVAL_SEC;
    }

    public void start() {
        if(ConfigurationService.get(METRICS_SERVER_NAME).toLowerCase().equals(GRAPHITE)) {
            graphiteReporter.start(metricsReportIntervalSec, TimeUnit.SECONDS);
        }
        if(ConfigurationService.get(METRICS_SERVER_NAME).toLowerCase().equals(GANGLIA)) {
            gangliaReporter.start(metricsReportIntervalSec, TimeUnit.SECONDS);
        }
    }

    public void stop() {
        if (graphiteReporter != null) {
            try {
                // reporting final metrics into graphite before stopping
                graphiteReporter.report();
            } finally {
                graphiteReporter.stop();
            }
        }
        if (gangliaReporter != null) {
            try {
                // reporting final metrics into graphite before stopping
                gangliaReporter.report();
            } finally {
                gangliaReporter.stop();
            }
        }
    }

}
