package com.mas2022datascience.generalworkermva.processor;

import com.mas2022datascience.avro.v1.PlayerBall;
import java.time.Instant;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class PlayerBallEventTimestampExtractor implements TimestampExtractor {
  @Override
  public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {

    PlayerBall playerBall = (PlayerBall) record.value();
    long eventTime = Instant.from(playerBall.getTs()).toEpochMilli();

    if (record.value() instanceof PlayerBall) {
      return eventTime;
    }
    return previousTimestamp;
  }
}
