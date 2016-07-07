/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

/**
 * Implements a SerializationSchema and DeserializationSchema for TaxiRide for Kafka data sources and sinks.
 */
public class CollectdRecordSchema implements DeserializationSchema<CollectdRecord>, SerializationSchema<CollectdRecord> {

	@Override
	public byte[] serialize(CollectdRecord element) {
		return element.toString().getBytes();
	}

	@Override
	public CollectdRecord deserialize(byte[] message) {
		return CollectdRecord.fromJson(new String(message));
	}

	@Override
	public boolean isEndOfStream(CollectdRecord nextElement) {
		return false;
	}

	@Override
	public TypeInformation<CollectdRecord> getProducedType() {
		return TypeExtractor.getForClass(CollectdRecord.class);
	}
}