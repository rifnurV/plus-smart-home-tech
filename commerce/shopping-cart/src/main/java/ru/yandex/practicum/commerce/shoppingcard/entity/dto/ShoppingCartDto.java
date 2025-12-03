package ru.yandex.practicum.commerce.shoppingcard.entity.dto;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;
import java.util.UUID;

@Getter
@Setter
@Builder
public class ShoppingCartDto {
    private final UUID shoppingCartId;
    private final Map<UUID, Long> products;
}
