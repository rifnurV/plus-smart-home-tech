package ru.yandex.practicum.telemetry.analyzer.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.telemetry.analyzer.config.KafkaConfig;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class SnapshotProcessor implements Runnable {
    private final SnapshotAnalyser analyser;
    private final KafkaConsumer<String, SensorsSnapshotAvro> consumer;
    private final List<String> topics;
    private final Duration pollTimeout;

    // Хранилище для оффсетов, которые мы хотим зафиксировать вручную
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    public SnapshotProcessor(SnapshotAnalyser analyser, KafkaConfig kafkaConfig) {
        this.analyser = analyser;

        final String componentName = this.getClass().getSimpleName();
        final KafkaConfig.ConsumerConfig consumerConfig =
                kafkaConfig.getConsumers().get(componentName);

        if (consumerConfig == null) {
            throw new IllegalStateException("Конфигурация потребителя не найдена: " + componentName);
        }

        this.consumer = new KafkaConsumer<>(consumerConfig.getProperties());
        this.topics = consumerConfig.getTopics();
        this.pollTimeout = consumerConfig.getPollTimeout();

        // Регистрация хука
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Завершение JVM. Прерываю работу консьюмера снапшотов.");
            consumer.wakeup();
        }, "KafkaShutdownHook-" + componentName));
    }

    @Override
    public void run() {
        start();
    }

    public void start() {
        log.info("Запуск потребителя снапшотов. Подписываюсь на топики: {}", topics);
        try {
            consumer.subscribe(topics);

            while (true) {
                // Блокирующий вызов.
                ConsumerRecords<String, SensorsSnapshotAvro> records = consumer.poll(pollTimeout);

                if (records.isEmpty()) {
                    log.trace("Нет новых записей. Ожидаем.");
                    continue;
                }

                log.debug("Обрабатываю {} записей-снапшотов.", records.count());

                int processedCount = 0;
                for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                    try {
                        analyser.process(record.value());

                        updateOffsets(record);

                        processedCount++;
                        if (processedCount % 100 == 0) {
                            commitAsyncPeriodically();
                        }

                    } catch (Exception e) {
                        log.error("Ошибка при обработке записи из топика: {}, партиции: {}, смещения: {}",
                                record.topic(), record.partition(), record.offset(), e);
                    }
                }

                consumer.commitAsync(currentOffsets, (offsets, exception) -> {
                    if (exception != null) {
                        log.warn("Ошибка при асинхронной фиксации оффсетов пакета: {}", offsets, exception);
                    }
                });
            }

        } catch (WakeupException ignored) {
            log.info("Получен сигнал Wakeup. Завершаю работу консьюмера.");
        } catch (Exception e) {
            log.error("Критическая ошибка во время работы потребителя Kafka", e);
        } finally {
            try {
                log.info("Синхронная фиксация оставшихся оффсетов перед закрытием: {}", currentOffsets);
                consumer.commitSync(currentOffsets);
            } catch (Exception e) {
                log.error("Ошибка при финальной синхронной фиксации оффсетов", e);
            } finally {
                log.info("Закрываю Kafka-консьюмер.");
                consumer.close();
            }
        }
    }


    private void updateOffsets(ConsumerRecord<String, SensorsSnapshotAvro> record) {
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        OffsetAndMetadata offset = new OffsetAndMetadata(record.offset() + 1);
        currentOffsets.put(tp, offset);
        log.trace("Обновлен оффсет для {} на {}", tp, offset.offset());
    }

    private void commitAsyncPeriodically() {
        if (!currentOffsets.isEmpty()) {
            consumer.commitAsync(currentOffsets, (offsets, exception) -> {
                if (exception != null) {
                    log.warn("Ошибка во время фиксации оффсетов: {}", offsets, exception);
                } else {
                    log.debug("Коммит {} оффсетов успешен.", offsets.size());
                }
            });
        }
    }
}