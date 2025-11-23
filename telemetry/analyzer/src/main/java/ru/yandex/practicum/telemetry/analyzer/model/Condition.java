package ru.yandex.practicum.telemetry.analyzer.model;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro;

@Entity
@Getter
@Setter
@Table(name = "conditions")
public class Condition {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Enumerated(EnumType.STRING)
    private ConditionTypeAvro type;

    @Enumerated(EnumType.STRING)
    private ConditionOperation operation;

    private Integer value;

    public boolean check(Integer sensorValue) {
        if (sensorValue == null || operation == null || value == null) {
            return false;
        }
        return switch (operation) {
            case EQUALS -> sensorValue.equals(value);
            case GREATER_THAN -> sensorValue > value;
            case LOWER_THAN -> sensorValue < value;
        };
    }
}
