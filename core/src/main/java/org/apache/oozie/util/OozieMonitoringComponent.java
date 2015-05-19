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

package org.apache.oozie.util;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import org.apache.oozie.service.PopulatorMetricsReportingManagerService;

import java.util.HashMap;
import java.util.Map;

public class OozieMonitoringComponent {
    public static final char DOT = '.';
    public static final char HIPHEN = '-';
    private PopulatorMetricsReportingManagerService metricsReporter;
    private MetricRegistry metricRegistry;
    private Map<String, Counter> counterMap;

    public OozieMonitoringComponent() {
        metricRegistry = new MetricRegistry();
        metricsReporter = new PopulatorMetricsReportingManagerService();
        metricsReporter.init(metricRegistry);
        metricsReporter.start();
        counterMap = new HashMap<String, Counter>();
    }

    public void initializeCounter(String group, String name) {
        group = group.replace(DOT,HIPHEN);
        name = name.replace(DOT,HIPHEN);
        String counterName = group.concat(".").concat(name);
        Counter counter = metricRegistry.counter(MetricRegistry.name(counterName));
        counterMap.put(counterName, counter);
    }

    public void incrCounter(String group, String name, long count) {
        group = group.replace(DOT,HIPHEN);
        name = name.replace(DOT,HIPHEN);
        String counterName = group.concat(".").concat(name);
        Counter counter = counterMap.get(counterName);
        counter.inc(count);
    }

    public void monitorVariable(String group, String name, Instrumentation.Variable variable) {
        if (group.equals("jobstatus") || group.equals("jvm") || group.equals("locks") ||
                group.equals("windowjobstatus")) {
            group = group.replace(DOT,HIPHEN);
            name = name.replace(DOT,HIPHEN);
            String counterName = group.concat(".").concat(name);
            final Instrumentation.Variable<Long> value = variable;
            metricRegistry.register(MetricRegistry.name(counterName), new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return value.getValue();
                }
            });
        }
    }

    public void monitorSampler(String group, String name, Instrumentation.Variable<Long> variable) {
        final Instrumentation.Variable<Long> value = variable;
        group = group.replace(DOT,HIPHEN);
        name = name.replace(DOT,HIPHEN);
        String counterName = group.concat(".").concat(name);
        metricRegistry.register(MetricRegistry.name(counterName), new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return value.getValue();
                    }
                });
    }

}
