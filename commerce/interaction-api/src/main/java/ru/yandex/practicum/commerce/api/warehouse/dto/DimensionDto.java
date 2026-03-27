package ru.yandex.practicum.commerce.api.warehouse.dto;

import jakarta.validation.constraints.Min;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@Builder
public class DimensionDto {
    @Min(value = 1, message = "width должен быть минимум 1")
    private double width;

    @Min(value = 1, message = "height должен быть минимум 1")
    private double height;

    @Min(value = 1, message = "depth должен быть минимум 1")
    private double depth;
}
