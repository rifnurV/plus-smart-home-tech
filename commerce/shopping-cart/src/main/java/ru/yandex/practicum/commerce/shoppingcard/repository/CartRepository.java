package ru.yandex.practicum.commerce.shoppingcard.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.commerce.shoppingcard.entity.ShoppingCart;

import java.util.Optional;
import java.util.UUID;

public interface CartRepository extends JpaRepository<ShoppingCart, UUID> {
    Optional<ShoppingCart> findByUsernameAndActive(String username, boolean active);
}
