package ru.yandex.practicum.commerce.shoppingstore.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import ru.yandex.practicum.commerce.api.store.enums.ProductCategory;
import ru.yandex.practicum.commerce.api.store.enums.ProductState;
import ru.yandex.practicum.commerce.api.store.enums.QuantityState;

import java.util.UUID;

@Getter
@Setter
@Entity
@Table(name = "products")
public class Product {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private UUID productId;

    @Column(nullable = false)
    private String productName;

    @Column(nullable = false)
    private String description;

    private String imageSrc;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private QuantityState quantityState;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private ProductState productState;

    @Column(nullable = false)
    private double rating;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private ProductCategory productCategory;

    @Column(nullable = false)
    private Double price;
}
