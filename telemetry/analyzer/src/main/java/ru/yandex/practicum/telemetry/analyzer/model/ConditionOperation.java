package ru.yandex.practicum.telemetry.analyzer.model;

import ru.yandex.practicum.kafka.telemetry.event.ConditionOperationAvro;

public enum ConditionOperation {
    EQUALS,
    GREATER_THAN,
    LOWER_THAN;

    public static ConditionOperation from(ConditionOperationAvro avroOperation) {
        if (avroOperation == null) {
            throw new IllegalArgumentException("Операция должен быть заполнен");
        }
        switch (avroOperation) {
            case EQUALS:
                return EQUALS;
            case GREATER_THAN:
                return GREATER_THAN;
            case LOWER_THAN:
                return LOWER_THAN;
            default:
                throw new IllegalArgumentException("Неизвестная операция: " + avroOperation);
        }
    }
}
