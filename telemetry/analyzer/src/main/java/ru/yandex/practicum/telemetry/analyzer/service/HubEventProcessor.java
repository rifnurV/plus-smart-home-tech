package ru.yandex.practicum.telemetry.analyzer.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;
import ru.yandex.practicum.telemetry.analyzer.config.KafkaConfig;
import ru.yandex.practicum.telemetry.analyzer.model.Sensor;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

@Slf4j
@Component
public class HubEventProcessor implements Runnable {
    private final KafkaConsumer<String, HubEventAvro> consumer;
    private final SensorService sensorService;
    private final ScenarioService scenarioService;
    private final List<String> topics;
    private final Duration pollTimeout;

    public HubEventProcessor(KafkaConfig config, SensorService sensorService, ScenarioService scenarioService) {
        this.sensorService = sensorService;
        this.scenarioService = scenarioService;

        final KafkaConfig.ConsumerConfig consumerConfig = config.getConsumers().get(this.getClass().getSimpleName());

        if (consumerConfig == null) {
            throw new IllegalStateException("Конфигурация consumer не найдена: " + this.getClass().getSimpleName());
        }

        this.consumer = new KafkaConsumer<>(consumerConfig.getProperties());
        this.topics = consumerConfig.getTopics();
        this.pollTimeout = consumerConfig.getPollTimeout();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Завершение JVM. Прерываю работу consumer.");
            consumer.wakeup();
        }, "KafkaShutdownHook-" + this.getClass().getSimpleName()));
    }

    @Override
    public void run() {
        log.info("Сonsumer подписывается на топики: {}", topics);
        consumer.subscribe(topics);
        try {
            while (true) {
                ConsumerRecords<String, HubEventAvro> records = consumer.poll(pollTimeout);

                if (records.isEmpty()) {
                    log.trace("Нет новых записей. Ожидаем.");
                    continue;
                }

                log.debug("Получаю {} записей.", records.count());

                records.forEach(record -> {
                    try {
                        processEvent(record.value());
                    } catch (Exception e) {
                        log.error("Ошибка при обработке записи из топика: {}, партиции: {}, смещения: {}",
                                record.topic(), record.partition(), record.offset(), e);
                    }
                });

                try {
                    consumer.commitSync();
                    log.trace("Офсеты зафиксированы.");
                } catch (Exception e) {
                    log.error("Ошибка при фиксации офсетов", e);
                }
            }
        } catch (WakeupException e) {
            log.info("Завершаю работу consumer.");
        } catch (Exception e) {
            log.error("Критическая ошибка во время работы consumer", e);
        } finally {
            log.info("Закрываю consumer.");
            consumer.close();
        }
    }

    private void processEvent(HubEventAvro hubEvent) {
        String hubId = hubEvent.getHubId();

        switch (hubEvent.getPayload()) {
            case DeviceAddedEventAvro deviceAddedEventAvro -> processEvent(hubId, deviceAddedEventAvro);
            case DeviceRemovedEventAvro deviceRemovedEventAvro -> processEvent(hubId, deviceRemovedEventAvro);
            case ScenarioAddedEventAvro scenarioAddedEventAvro -> processEvent(hubId, scenarioAddedEventAvro);
            case ScenarioRemovedEventAvro scenarioRemovedEventAvro -> processEvent(hubId, scenarioRemovedEventAvro);
            default -> log.warn("Получено событие неизвестного типа: {}", hubEvent);
        }
    }

    private void processEvent(String hubId, DeviceAddedEventAvro event) {
        String deviceId = event.getId();

        Optional<Sensor> maybeAdded = sensorService.findByIdAndHubId(hubId, deviceId);
        if (maybeAdded.isPresent()) {
            log.warn("Устройство с id [{}] уже зарегистрировано в хабе [{}]. Пропускаю.", deviceId, hubId);
            return;
        }

        Sensor sensor = new Sensor();
        sensor.setHubId(hubId);
        sensor.setId(deviceId);

        log.info("В хабе [{}] зарегистрирован новый датчик: [{}]", hubId, deviceId);
        sensorService.save(sensor);
    }

    private void processEvent(String hubId, DeviceRemovedEventAvro event) {
        String deviceId = event.getId();

        log.info("Удаляю датчик [{}] из хаба [{}]", deviceId, hubId);

        sensorService.findByIdAndHubId(hubId, deviceId)
                .ifPresentOrElse(
                        sensorService::delete,
                        () -> log.warn("Датчик [{}] не найден в хабе [{}]. Удаление не требуется.", deviceId, hubId)
                );
    }

    private void processEvent(String hubId, ScenarioAddedEventAvro event) {
        log.info("Запрос на добавление сценария '{}' для хаба {}", event.getName(), hubId);
        scenarioService.save(event, hubId);
    }

    private void processEvent(String hubId, ScenarioRemovedEventAvro event) {
        log.info("Запрос на удаление сценария '{}' из хаба {}", event.getName(), hubId);
        scenarioService.delete(event.getName(), hubId);
    }
}