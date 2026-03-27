package ru.yandex.practicum.commerce.api.warehouse.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.UUID;

@Getter
@Setter
@AllArgsConstructor
public class ShippedToDeleveryRequest {
    UUID orderId;
    UUID deliveryId;
}
