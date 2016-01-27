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
package com.dataartisans;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.json.JSONParseFlatMap;
import org.apache.flink.streaming.connectors.json.JSONParser;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.apache.sling.commons.json.JSONException;

/**
 * Read collectd with a Kafka consumer
 *
 * Note that the Kafka source is expecting the following parameters to be set
 *  - "bootstrap.servers" (comma separated list of kafka brokers)
 *  - "zookeeper.connect" (comma separated list of zookeeper servers)
 *  - "group.id" the id of the consumer group
 *  - "topic" the name of the topic to read data from.
 *
 * You can pass these required parameters using "--bootstrap.servers host:port,host1:port1 --zookeeper.connect host:port --topic testTopic"
 *
 * This is a valid input example:
 * 		--topic collectd --bootstrap.servers localhost:9092 --zookeeper.connect localhost:2181 --group.id myGroup
 *
 *
 */
public class ReadFromKafka {

	public static void main(String[] args) throws Exception {
		// create execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// parse user parameters
		ParameterTool parameterTool = ParameterTool.fromArgs(args);

		DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer082<>(parameterTool.getRequired("topic"),
				new SimpleStringSchema(), parameterTool.getProperties()));

		// print() will write the contents of the stream to the TaskManager's standard out stream
		// the re-balance call is causing a repartitioning of the data so that all machines
		// see the messages (for example in cases when "num kafka partitions" < "num flink operators"
		messageStream.flatMap(new SelectFreeMemoryFlatMap()).print();

		env.execute();
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	/**
	 * Makes sentences from English tweets.
	 * <p/>
	 * <p>
	 * Implements a string tokenizer that splits sentences into words as a
	 * user-defined FlatMapFunction. The function takes a line (String) and
	 * splits it into multiple pairs in the form of "(word,1)" (Tuple2<String,
	 * Integer>).
	 * </p>
	 */
	public static class SelectFreeMemoryFlatMap extends JSONParseFlatMap<String, Tuple2<String, Long>> {
		private static final long serialVersionUID = 1L;

		/**
		 * Select the language from the incoming JSON text
		 */
		@Override
		public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
			try {
				String valueTrim = value.substring(1, value.length());
				JSONParser parser = new JSONParser(valueTrim);
				if (parser.parse("type_instance").getString("retValue").equals("free")) {
					out.collect(new Tuple2<>(parser.parse("host").getString("retValue"),
							parser.parse("values").getJSONArray("retValue").getLong(0)));
				}
			} catch (JSONException e) {
				// the JSON was not parsed correctly
			}
		}
	}
}
