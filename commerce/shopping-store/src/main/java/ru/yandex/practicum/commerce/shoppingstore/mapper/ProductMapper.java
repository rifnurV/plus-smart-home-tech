package ru.yandex.practicum.commerce.shoppingstore.mapper;

import org.mapstruct.InjectionStrategy;
import org.mapstruct.Mapper;
import org.mapstruct.MappingConstants;
import ru.yandex.practicum.commerce.api.store.dto.ProductDto;
import ru.yandex.practicum.commerce.shoppingstore.entity.Product;


@Mapper(componentModel = MappingConstants.ComponentModel.SPRING, injectionStrategy = InjectionStrategy.CONSTRUCTOR)
public interface ProductMapper {
    Product toProduct(final ProductDto productDto);

    ProductDto toProductDto(final Product product);
}