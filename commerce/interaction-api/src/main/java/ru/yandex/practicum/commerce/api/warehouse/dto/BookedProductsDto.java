package ru.yandex.practicum.commerce.api.warehouse.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@Builder
public class BookedProductsDto {
    private double deliveryWeight;
    private double deliveryVolume;
    private boolean fragile;
}
