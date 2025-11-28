package ru.yandex.practicum.telemetry.aggregator.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class SnapshotServiceImpl implements SnapshotService {
    // Хранит снимки состояния
    private final Map<String, SensorsSnapshotAvro> sensors = new HashMap<>();

    @Override
    public Optional<SensorsSnapshotAvro> updateState(SensorEventAvro event) {
        // Находим снапшот состояния датчиков конкретного хаба.
        // Если снэпшот ещё не вычислялся - создаём новый.
        final SensorsSnapshotAvro snapshot = sensors.computeIfAbsent(
                event.getHubId(),
                hubId ->  // создаём новый снапшот
                        SensorsSnapshotAvro.newBuilder()
                                .setHubId(hubId)
                                .setTimestamp(event.getTimestamp())
                                .setSensorsState(new HashMap<>())
                                .build()
        );

        // берём мапу с данными всех устройств данного хаба
        // и кладём в переменную, для удобства
        Map<String, SensorStateAvro> sensorsState = snapshot.getSensorsState();

        // Если для данного устройства уже были получены данные ранее,
        // то проверяем изменяют ли новые данные старое состояние
        if(sensorsState.containsKey(event.getId())) {
            SensorStateAvro oldState = sensorsState.get(event.getId());
            // если таймстемп у нового события раньше, чем таймстемп текущего состояния,
            // или данные нового события ничего не меняют по сравнению с текущими данными,
            // то игнорируем новое событие и ничего не обновляем
            if(oldState.getTimestamp().isAfter(event.getTimestamp()) ||
                    oldState.getPayload().equals(event.getPayload())) {
                return Optional.empty();
            }
        }

        // Если дошли сюда, значит пришли новые данные и
        // снапшот нужно обновить
        SensorStateAvro newState = SensorStateAvro.newBuilder()
                .setTimestamp(event.getTimestamp())
                .setPayload(event.getPayload())
                .build();
        sensorsState.put(event.getId(), newState);

        snapshot.setTimestamp(newState.getTimestamp());
        return Optional.of(snapshot);
    }
}
