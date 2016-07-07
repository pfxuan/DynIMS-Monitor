/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.clemson.bigdata.tls;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;


/**
 * Sending eviction trigger signal to TLS-controller via Kafka.
 *
 * The following arguments are required:
 *
 *  - "bootstrap.servers" (comma separated list of kafka brokers)
 *  - "topic" the name of the topic to write data to.
 *
 * This is an example command line argument:
 *  "--topic test --bootstrap.servers localhost:9092"
 */
public class WriteIntoKafka {
	public static void main(String[] args) throws Exception {
		// create execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().disableSysoutLogging();
		env.setNumberOfExecutionRetries(4);
		env.setParallelism(2);

		// parse user parameters
		ParameterTool parameterTool = ParameterTool.fromArgs(args);

		// add a simple source which is writing some strings
		// very simple data generator
		DataStream<String> messageStream = env.addSource(new SourceFunction<String>() {
			public boolean running = true;

			@Override
			public void run(SourceContext<String> ctx) throws Exception {
				long i = 0;
				while(this.running) {
					ctx.collect("Element - " + i++);
					Thread.sleep(500);
				}
			}

			@Override
			public void cancel() {
				running = false;
			}
		});

		// write data into Kafka
		messageStream.addSink(new FlinkKafkaProducer09<>(
				parameterTool.getRequired("topic"),
				new SimpleStringSchema(),
				parameterTool.getProperties()));

		env.execute("Write into Kafka example");
	}
}


