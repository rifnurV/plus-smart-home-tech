package ru.yandex.practicum.telemetry.analyzer.service;

import com.google.protobuf.Timestamp;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.analyzer.model.Action;
import ru.yandex.practicum.telemetry.analyzer.model.Condition;
import ru.yandex.practicum.telemetry.analyzer.model.Scenario;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioRepository;

import java.time.Instant;
import java.util.Map;

import static ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc.HubRouterControllerBlockingStub;
import static ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro.TEMPERATURE;
import static ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro.LUMINOSITY;
import static ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro.MOTION;
import static ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro.SWITCH;


@Slf4j
@Service
@Transactional(readOnly = true)
public class SnapshotAnalyser {
    private final ScenarioRepository scenarioRepository;
    private final HubRouterControllerBlockingStub hubRouterClient;

    public SnapshotAnalyser(ScenarioRepository scenarioRepository,
                            @GrpcClient("hub-router") HubRouterControllerBlockingStub hubRouterClient) {
        this.scenarioRepository = scenarioRepository;
        this.hubRouterClient = hubRouterClient;
    }

    // Находит все сценарии для хаба и выполняет действия, если условия совпали.
    public void process(SensorsSnapshotAvro snapshot) {
        scenarioRepository.findByHubId(snapshot.getHubId())
                .stream()
                .filter(scenario -> isConditionsMatchSnapshot(snapshot, scenario.getConditions()))
                .forEach(this::performActions);
    }

    private boolean isConditionsMatchSnapshot(SensorsSnapshotAvro snapshot, Map<String, Condition> conditions) {
        return conditions.entrySet().stream()
                .allMatch(conditionEntry ->
                        checkCondition(conditionEntry.getKey(), conditionEntry.getValue(), snapshot)
                );
    }

    // Проверяет соответствие условия текущему состоянию датчика.
    private boolean checkCondition(String sensorId, Condition condition, SensorsSnapshotAvro snapshot) {
        final SensorStateAvro state = snapshot.getSensorsState().get(sensorId);

        if (state == null) {
            log.trace("Условия для датчика [{}]: состояние не найдено в снапшоте.", sensorId);
            return false;
        }

        Object sensorValue = extractSensorValue(state.getPayload(), condition.getType());

        if (sensorValue == null) {
            log.warn("Нет значений типа {} из датчика {}.",
                    condition.getType(), sensorId);
            return false;
        }

        Integer checkValue = convertValueToCheckType(sensorValue);

        return checkValue != null && condition.check(checkValue);
    }

    //Извлекает значение из конкретного payload Avro в зависимости от требуемого ConditionTypeAvro.
    private Object extractSensorValue(Object payload, ConditionTypeAvro type) {
        return switch (payload) {
            case ClimateSensorAvro data -> switch (type) {
                case TEMPERATURE -> data.getTemperatureC();
                case CO2LEVEL -> data.getCo2Level();
                case HUMIDITY -> data.getHumidity();
                default -> null;
            };
            case LightSensorAvro data -> type.equals(LUMINOSITY) ? data.getLuminosity() : null;
            case MotionSensorAvro data -> type.equals(MOTION) ? data.getMotion() : null;
            case TemperatureSensorAvro data -> type.equals(TEMPERATURE) ? data.getTemperatureC() : null;
            case SwitchSensorAvro data -> type.equals(SWITCH) ? data.getState() : null;
            default -> null;
        };
    }

    //Конвертирует извлеченное значение в тип Integer, используемый в Condition.check().
    private Integer convertValueToCheckType(Object value) {
        if (value == null) {
            return null;
        }
        return switch (value) {
            case Integer i -> i;
            case Boolean b -> b ? 1 : 0;
            case Double d -> d.intValue();
            case Float f -> f.intValue();
            default -> {
                log.warn("Неизвестный тип значения для конвертации в Integer: {}", value.getClass().getName());
                yield null;
            }
        };
    }

    //Выполняет все действия, связанные со сработавшим сценарием, через gRPC./
    private void performActions(Scenario scenario) {
        log.info("Сработал сценарий '{}' для хаба [{}]. Выполняю {} действий.",
                scenario.getName(), scenario.getHubId(), scenario.getActions().size());

        final Timestamp timestamp = buildCurrentTimestamp();

        for (Map.Entry<String, Action> actionEntry : scenario.getActions().entrySet()) {
            Action scenarioAction = actionEntry.getValue();
            String sensorId = actionEntry.getKey();

            DeviceActionProto deviceAction = buildDeviceAction(sensorId, scenarioAction);

            try {
                hubRouterClient.handleDeviceAction(
                        DeviceActionRequest.newBuilder()
                                .setHubId(scenario.getHubId())
                                .setScenarioName(scenario.getName())
                                .setAction(deviceAction)
                                .setTimestamp(timestamp)
                                .build()
                );
            } catch (Exception e) {
                log.error("Ошибка при отправке gRPC действия [{}] для хаба [{}] и устройства [{}].",
                        scenarioAction.getType().name(), scenario.getHubId(), sensorId, e);
            }
        }
    }

    private Timestamp buildCurrentTimestamp() {
        Instant ts = Instant.now();
        return Timestamp.newBuilder()
                .setSeconds(ts.getEpochSecond())
                .setNanos(ts.getNano())
                .build();
    }

    private DeviceActionProto buildDeviceAction(String sensorId, Action scenarioAction) {
        DeviceActionProto.Builder actionBuilder = DeviceActionProto.newBuilder()
                .setSensorId(sensorId)
                .setType(mapActionType(scenarioAction.getType()));

        if (ActionTypeAvro.SET_VALUE.equals(scenarioAction.getType()) && scenarioAction.getValue() != null) {
            actionBuilder.setValue(scenarioAction.getValue());
        }

        return actionBuilder.build();
    }

    private ActionTypeProto mapActionType(ActionTypeAvro avro) {
        try {
            return ActionTypeProto.valueOf(avro.name());
        } catch (IllegalArgumentException e) {
            log.error("Неизвестный тип действия Avro: {}", avro.name());
            throw new IllegalArgumentException("Неизвестный тип действия: " + avro.name());
        }
    }
}