package com.example.demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.redis.connection.ReactiveRedisConnection;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.ReactiveStringCommands;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.UUID;

@SpringBootApplication
public class DemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);

    }
}

@RestController
class TestController {
    private final ReactiveRedisConnectionFactory connectionFactory;
    private ReactiveRedisConnection connection;
    private final RedisSerializer<String> serializer = new StringRedisSerializer();

    @Autowired
    public TestController(ReactiveRedisConnectionFactory connectionFactory) {
        this.connection = connectionFactory.getReactiveConnection();
        this.connectionFactory = connectionFactory;
    }

    @GetMapping("/hello")
    public Flux<String> hello() {
        Flux<String> keyFlux = Flux.range(0, 50).map(i -> ("Key-" + i));

        Flux<ReactiveStringCommands.SetCommand> generator = keyFlux.map(String::getBytes).map(ByteBuffer::wrap)
                .map(key -> ReactiveStringCommands.SetCommand.set(key)
                        .value(ByteBuffer.wrap(UUID.randomUUID().toString().getBytes())));

        connection.stringCommands()
                .set(generator)
                .then()
                .block();

        return connection.keyCommands()
                .keys(ByteBuffer.wrap(serializer.serialize("Key*")))
                .flatMapMany(Flux::fromIterable)
                .map(TestController::toString);
    }

    private static String toString(ByteBuffer byteBuffer) {

        byte[] bytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytes);
        return new String(bytes);
    }
}