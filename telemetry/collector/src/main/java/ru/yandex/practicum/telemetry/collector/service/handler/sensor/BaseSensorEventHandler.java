package ru.yandex.practicum.telemetry.collector.service.handler.sensor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducer;
import ru.yandex.practicum.telemetry.collector.service.handler.SensorEventHandler;

import java.time.Instant;

import static ru.yandex.practicum.telemetry.collector.configuration.KafkaConfig.TopicType.SENSORS_EVENTS;

@Slf4j
@RequiredArgsConstructor
public abstract class BaseSensorEventHandler<T extends SpecificRecordBase> implements SensorEventHandler {
    protected final KafkaEventProducer producer;

    protected abstract T mapToAvro(SensorEventProto event);

    @Override
    public void handle(SensorEventProto event) {
        if (!event.getPayloadCase().equals(getMessageType())) {
            throw new IllegalArgumentException("Неизвестный тип события: " + event.getPayloadCase());
        }

        T payload = mapToAvro(event);

        Instant timestamp = Instant.ofEpochSecond(
                event.getTimestamp().getSeconds(), event.getTimestamp().getNanos()
        );

        SensorEventAvro sensorEventAvro = SensorEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setId(event.getId())
                .setTimestamp(timestamp)
                .setPayload(payload)
                .build();

        System.out.println(String.format("sensorEventAvro: %s, event.getHubId: %s, timestamp: %s, SENSORS_EVENTS: %s",
                sensorEventAvro, event.getHubId(), timestamp, SENSORS_EVENTS));

        producer.send(sensorEventAvro, event.getHubId(), timestamp, SENSORS_EVENTS);
    }
}