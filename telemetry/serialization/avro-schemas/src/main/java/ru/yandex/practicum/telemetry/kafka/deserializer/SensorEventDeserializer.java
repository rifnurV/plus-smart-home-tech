package ru.yandex.practicum.telemetry.kafka.deserializer;

import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

/**
 * Десериализатор для событий SensorEventAvro.
 * Расширяет базовый класс, чтобы избежать дублирования кода.
 */
public class SensorEventDeserializer extends BaseAvroDeserializer<SensorEventAvro> {

    // Конструктор по умолчанию, который получает схему из сгенерированного класса
    public SensorEventDeserializer() {
        super(SensorEventAvro.getClassSchema());
    }
}