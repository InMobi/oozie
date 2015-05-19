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

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;

public class PopulatorMetricsReportingManagerService {

    private String GRAPHITE_HOST;
    private String GRAPHITE_METRICS_PREFIX;

    private long GRAPHITE_REPORT_INTERVAL_SEC;
    private int GRAPHITE_PORT;

    private GraphiteReporter graphiteReporter = null;
    private long graphiteReportIntervalSec;

    public PopulatorMetricsReportingManagerService() {
        GRAPHITE_HOST=ConfigurationService.get("GRAPHITE_HOST");
        GRAPHITE_METRICS_PREFIX=ConfigurationService.get("GRAPHITE_METRICS_PREFIX");
        GRAPHITE_REPORT_INTERVAL_SEC=ConfigurationService.getLong("GRAPHITE_REPORT_INTERVAL_SEC");
        GRAPHITE_PORT=ConfigurationService.getInt("GRAPHITE_PORT");
    }

    public void init(MetricRegistry metricRegistry) {
        // Initialize graphite reporting related objects
        Graphite graphite = new Graphite(new InetSocketAddress(GRAPHITE_HOST, GRAPHITE_PORT));
        graphiteReporter =
                GraphiteReporter.forRegistry(metricRegistry).prefixedWith(GRAPHITE_METRICS_PREFIX)
                        .convertDurationsTo(TimeUnit.SECONDS).filter(MetricFilter.ALL).build(graphite);
        graphiteReportIntervalSec = GRAPHITE_REPORT_INTERVAL_SEC;

    }

    public void start() {
        graphiteReporter.start(graphiteReportIntervalSec, TimeUnit.SECONDS);
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
    }

}
