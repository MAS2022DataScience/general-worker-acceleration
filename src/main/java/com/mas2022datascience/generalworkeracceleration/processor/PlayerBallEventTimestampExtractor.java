package com.mas2022datascience.generalworkeracceleration.processor;

import com.mas2022datascience.avro.v1.PlayerBall;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class PlayerBallEventTimestampExtractor implements TimestampExtractor {
  @Override
  public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {

    return ((PlayerBall)record.value()).getTs().toEpochMilli();

  }
}
