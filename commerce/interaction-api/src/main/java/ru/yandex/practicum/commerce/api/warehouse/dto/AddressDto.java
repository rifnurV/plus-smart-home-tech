package ru.yandex.practicum.commerce.api.warehouse.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class AddressDto {
    private String country;
    private String city;
    private String street;
    private String house;
    private String flat;
}
