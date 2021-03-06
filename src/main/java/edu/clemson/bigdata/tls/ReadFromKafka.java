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

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

/**
 * Read collectd with a Kafka consumer
 *
 * Note that the Kafka source is expecting the following parameters to be set
 *  - "bootstrap.servers" (comma separated list of kafka brokers)
 *  - "group.id" the id of the consumer group
 *  - "topic" the name of the topic to read data from.
 *
 * You can pass these required parameters using "--bootstrap.servers host:port,host1:port1 --topic testTopic"
 *
 * This is a valid input example:
 * 		--topic collectd --bootstrap.servers localhost:9092 --group.id myGroup
 *
 *
 */
public class ReadFromKafka {

	public static void main(String[] args) throws Exception {
		// Creates execution environment and configure it
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.registerType(CollectdRecord.class);

		env.getConfig().disableSysoutLogging();
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 1000L));
		//env.enableCheckpointing(5000);
		env.setParallelism(4);
		env.getConfig().setAutoWatermarkInterval(1000L);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		//env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		// Parses user parameters
		ParameterTool parameterTool = ParameterTool.fromArgs(args);

		// Adds Kafka source and assigns event timestamp
		DataStream<CollectdRecord> collectdStream = env
				.addSource(new FlinkKafkaConsumer09(
						parameterTool.getRequired("topic.in"),
						new CollectdRecordSchema(),
						parameterTool.getProperties())
				)
				// Assign timestamp using collectd event time
				.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<CollectdRecord>() {

					/** The current timestamp. */
					private long currentTimestamp = Long.MIN_VALUE;

					@Override
					public final long extractTimestamp(CollectdRecord element, long elementPrevTimestamp) {
						final long newTimestamp = element.getTime();
						if (newTimestamp >= this.currentTimestamp) {
							this.currentTimestamp = newTimestamp;
							return newTimestamp;
						} else {
							return newTimestamp;
						}
					}

					@Override
					public final Watermark getCurrentWatermark() {
						return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 1000);
					}
				});

		// Compute in-memory storage size
		DataStream<String> controllerStream = collectdStream
				.keyBy( record -> record.getHost() )
				.window(TumblingEventTimeWindows.of(Time.seconds(1)))
				//.timeWindow(Time.seconds(1), Time.seconds(1))
				.apply(new ComputeInMemorySize());


		// write the filtered data to a Kafka sink
		controllerStream.addSink(new FlinkKafkaProducer09<>(
				parameterTool.getRequired("topic.out"),
				new SimpleStringSchema(),
				parameterTool.getProperties()));

		env.execute("Read from Collectd");
	}

	public static class ComputeInMemorySize extends RichWindowFunction<
			CollectdRecord, 							// input type
			String,												// output type
			String,         							// key type
			TimeWindow>         			    // window type
	{

		/** The state handle for the size of in-memory storage space */
		private ValueState<Long> inMemSizeState;
		//private ValueState<Long> cachedMemSizeState;
		private static long TOTAL_MEM_SIZE = 135199723520L; // 125.9 GiB
		private static long RAMDISK_QUOTA = 64424509440L; // 60 GiB
		private static float LAMBDA = 0.50f;
		private static float MEM_UTILIZATION_REF = 0.95f;
		private static float FREE_MEM_REF = 1 - MEM_UTILIZATION_REF;
		private static long FREE_MEM_REF_SIZE = (long) ((1.0f - MEM_UTILIZATION_REF) * TOTAL_MEM_SIZE);
		private static long FREE_MEM_REF_DEVIATION_SIZE = 1024 * 1024 * 1024;
    private static long BLOCK_SIZE = 512 * 1024 * 1024;

		@Override
		public void open(Configuration config) throws Exception {
			inMemSizeState = getRuntimeContext().getState(
					new ValueStateDescriptor<Long>("inMemSizeState", LongSerializer.INSTANCE, RAMDISK_QUOTA)); // 60GB in-memory size
			//cachedMemSizeState = getRuntimeContext().getState(
			//		new ValueStateDescriptor<Long>("cachedMemSizeState", LongSerializer.INSTANCE, 0L)); // 0GB cached size
		}

		@Override
		public void apply(String host, TimeWindow window, Iterable<CollectdRecord> collectdRecords, Collector<String> out)
				throws Exception {
			long freeMemSize = -1L;
			long usedMemSize = 0L;
			long cachedMemSize = -1L;
			long freeRamdiskSize = 0L;
			long usedRamDiskSize = -1L;

			// Extract free, cached, buffer memory
			for (CollectdRecord record : collectdRecords) {
				switch (record.getPlugin()) {
					case "memory":
						switch (record.getType_instance()) {
							case "free":
								freeMemSize = record.getValues();
								usedMemSize = TOTAL_MEM_SIZE - freeMemSize;
								break;
							case "cached":
								cachedMemSize = record.getValues();
								break;
							default:
								break;
						}
						break;
					case "df":
						switch (record.getType_instance()) {
							case "free":
								freeRamdiskSize = record.getValues();
								break;
							case "used":
								usedRamDiskSize = record.getValues();
								break;
							default:
								break;
						}
						break;
					default:
						break;
				}
			}

			if ( freeMemSize != -1L && usedRamDiskSize != -1L
					&& Math.abs(freeMemSize - FREE_MEM_REF_SIZE ) >= FREE_MEM_REF_DEVIATION_SIZE) {
				// Calculate the next in-memory storage size
				// long nextInMemSize = inMemSizeState.value() - (long) ((float) LAMBDA * usedMemSize * ((float) usedMemSize / TOTAL_MEM_SIZE - FREE_MEM_REF) / FREE_MEM_REF);
				long nextInMemSize = inMemSizeState.value() - (long) (LAMBDA * (float) usedMemSize * ((float) usedMemSize / (float) TOTAL_MEM_SIZE - MEM_UTILIZATION_REF) / MEM_UTILIZATION_REF);
				//long nextInMemSize = usedRamDiskSize + (freeMemSize - FREE_MEM_REF_SIZE);
				nextInMemSize = (Math.floorDiv(nextInMemSize, BLOCK_SIZE) + 1) * BLOCK_SIZE;

				// In-memory storage size should be in the range of [0, RAMDISK_QUOTA]
				nextInMemSize = nextInMemSize <= 0L ? 0L : (nextInMemSize >= RAMDISK_QUOTA ? RAMDISK_QUOTA : nextInMemSize);

				if (nextInMemSize != inMemSizeState.value()) {
					// update in-memory size
					inMemSizeState.update(nextInMemSize);
					// Eviction size for data node
          long unusedInMemorySpace = (float) usedMemSize / (float) TOTAL_MEM_SIZE >= MEM_UTILIZATION_REF ? RAMDISK_QUOTA - usedRamDiskSize : 0L;
					out.collect(host + "\t" + Long.toString(RAMDISK_QUOTA - nextInMemSize + unusedInMemorySpace));
				} else {
					//out.collect("=======" + "\t" + nextInMemSize);
				}
			} else {
				//out.collect("=======" + inMemSizeState.value());
			}
		}

	}


	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************


}
