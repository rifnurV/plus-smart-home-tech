package ru.yandex.practicum.telemetry.collector.model.hub;

import jakarta.validation.constraints.NotBlank;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.FieldDefaults;
import ru.yandex.practicum.telemetry.collector.model.hub.enums.DeviceType;
import ru.yandex.practicum.telemetry.collector.model.hub.enums.HubEventType;

@Getter
@Setter
@FieldDefaults(level = AccessLevel.PRIVATE)
public class DeviceAddedEvent extends HubEvent {
    @NotBlank
    String Id;
    DeviceType deviceType;


    @Override
    public HubEventType getType() {
        return HubEventType.DEVICE_ADDED;
    }
}
