package ru.yandex.practicum.commerce.api.store.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import ru.yandex.practicum.commerce.api.store.enums.ProductCategory;
import ru.yandex.practicum.commerce.api.store.enums.ProductState;
import ru.yandex.practicum.commerce.api.store.enums.QuantityState;

import java.util.UUID;

@Getter
@Setter
@Builder
public class ProductDto {
    private UUID productId;

    @NotBlank
    private String productName;

    @NotBlank
    private String description;

    private String imageSrc;

    @NotNull
    private QuantityState quantityState;

    @NotNull
    private ProductState productState;

    private double rating;

    @NotNull(message = "Категория должа быть заполнена.")
    private ProductCategory productCategory;

    @Min(value = 1, message = "Цена должна быть больше 1.")
    @NotNull
    private Double price;
}
