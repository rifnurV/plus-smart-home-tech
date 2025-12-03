package ru.yandex.practicum.commerce.shoppingcard;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.openfeign.EnableFeignClients;

@SpringBootApplication
@EnableDiscoveryClient
@EnableFeignClients(basePackages = "ru.yandex.practicum")
public class ShoppingCart {
    public static void main(String[] args) {
        SpringApplication.run(ShoppingCart.class, args);
    }
}
