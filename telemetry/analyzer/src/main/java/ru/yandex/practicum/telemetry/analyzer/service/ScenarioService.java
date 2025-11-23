package ru.yandex.practicum.telemetry.analyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.ActionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.yandex.practicum.telemetry.analyzer.model.Action;
import ru.yandex.practicum.telemetry.analyzer.model.Condition;
import ru.yandex.practicum.telemetry.analyzer.model.ConditionOperation;
import ru.yandex.practicum.telemetry.analyzer.model.Scenario;
import ru.yandex.practicum.telemetry.analyzer.repository.ActionRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.ConditionRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.SensorRepository;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Service
@Transactional
@RequiredArgsConstructor
public class ScenarioService {
    private final ScenarioRepository scenarioRepository;
    private final ConditionRepository conditionRepository;
    private final ActionRepository actionRepository;
    private final SensorRepository sensorRepository;

    //Создает или обновляет сценарий на основе полученного события.
    public void save(ScenarioAddedEventAvro event, String hubId) {
        Set<String> sensors = collectSensorIds(event);
        checkAllSensorsExist(sensors, hubId);

        Scenario scenario = scenarioRepository.findByHubIdAndName(hubId, event.getName())
                .map(existingScenario -> updateExistingScenario(existingScenario))
                .orElseGet(() -> createNewScenario(event.getName(), hubId));

        mapAndAddConditions(event, scenario);
        mapAndAddActions(event, scenario);

        conditionRepository.saveAll(scenario.getConditions().values());
        actionRepository.saveAll(scenario.getActions().values());

        scenarioRepository.save(scenario);
        log.info("Сценарий '{}' хаба {} сохранен.", event.getName(), hubId);
    }

    //Удаляет сценарий по имени и ID хаба.
    public void delete(String name, String hubId) {
        scenarioRepository.findByHubIdAndName(hubId, name).ifPresentOrElse(scenario -> {
            conditionRepository.deleteAll(scenario.getConditions().values());
            actionRepository.deleteAll(scenario.getActions().values());
            scenarioRepository.delete(scenario);
            log.info("Сценарий '{}' хаба {} удален.", name, hubId);
        }, () -> {
            log.warn("Попытка удаления несуществующего сценария '{}' в хабе {}.", name, hubId);
        });
    }

    //Собирает все уникальные ID датчиков из условий и действий события.
    private Set<String> collectSensorIds(ScenarioAddedEventAvro event) {
        return Stream.concat(
                event.getConditions().stream().map(ScenarioConditionAvro::getSensorId),
                event.getActions().stream().map(DeviceActionAvro::getSensorId)
        ).collect(Collectors.toSet());
    }

    //Проверяет, существуют ли все датчики, упомянутые в сценарии, для данного хаба.
    private void checkAllSensorsExist(Set<String> sensors, String hubId) {
        boolean allSensorsExists = sensorRepository.existsByIdInAndHubId(sensors, hubId);
        if (!allSensorsExists) {
            log.error("Попытка создать сценарий с неизвестного устройства в хабе {}", hubId);
            throw new IllegalStateException("Нет возможности создать сценарий с неизвестного устройства");
        }
    }

    //Логика создания нового сценария.
    private Scenario createNewScenario(String name, String hubId) {
        log.info("Создание нового сценария '{}' для хаба {}.", name, hubId);
        Scenario scenario = new Scenario();
        scenario.setName(name);
        scenario.setHubId(hubId);
        return scenario;
    }

    //Удаление старых условий/действий.
    private Scenario updateExistingScenario(Scenario scenario) {
        log.info("Обновление сценария '{}' для хаба {}. Удаление старых связанных сущностей.",
                scenario.getName(), scenario.getHubId());

        Collection<Condition> oldConditions = scenario.getConditions().values();
        if (!oldConditions.isEmpty()) {
            conditionRepository.deleteAll(oldConditions);
            scenario.getConditions().clear();
        }

        Collection<Action> oldActions = scenario.getActions().values();
        if (!oldActions.isEmpty()) {
            actionRepository.deleteAll(oldActions);
            scenario.getActions().clear();
        }
        return scenario;
    }

    private void mapAndAddConditions(ScenarioAddedEventAvro event, Scenario scenario) {
        for (ScenarioConditionAvro eventCondition : event.getConditions()) {
            Condition condition = new Condition();
            condition.setType(eventCondition.getType());
            condition.setOperation(ConditionOperation.from(eventCondition.getOperation()));
            condition.setValue(mapValue(eventCondition.getValue()));
            scenario.addCondition(eventCondition.getSensorId(), condition);
        }
    }

    private void mapAndAddActions(ScenarioAddedEventAvro event, Scenario scenario) {
        for (DeviceActionAvro eventAction : event.getActions()) {
            Action action = new Action();
            action.setType(eventAction.getType());

            if (ActionTypeAvro.SET_VALUE.equals(eventAction.getType())) {
                action.setValue(mapValue(eventAction.getValue()));
            }
            scenario.addAction(eventAction.getSensorId(), action);
        }
    }

    private Integer mapValue(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof Integer i) {
            return i;
        }

        if (value instanceof Boolean b) {
            return b ? 1 : 0;
        }

        log.warn("Неподдерживаемый тип значения для маппинга: {}", value.getClass().getName());
        return null;
    }
}