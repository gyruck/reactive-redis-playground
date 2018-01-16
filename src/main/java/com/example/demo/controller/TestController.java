package com.example.demo.controller;

import com.example.demo.model.TestRequest;
import io.micrometer.core.annotation.Timed;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.ReactiveRedisConnection;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.ReactiveStringCommands;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.validation.Valid;
import java.nio.ByteBuffer;
import java.util.UUID;

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
    @Timed
    public Flux<String> hello() {

        return getEntry("1")
                .concatWith(getEntry("2"))
                .concatWith(getEntry("3"))
                .concatWith(getEntry("4"))
                .map(TestController::toString);
    }

    @GetMapping("/get/{id}")
    @Timed
    public Mono<String> getKey(@PathVariable String id) {

        return getEntry(id)
                .map(TestController::toString);
    }

    @PostMapping("/add")
    @Timed
    public Mono<ResponseEntity<Void>> add(@Valid @RequestBody TestRequest request) {
        Mono<ReactiveStringCommands.SetCommand> addCommand = Mono.just("Key-" + request.getId())
                .map(String::getBytes)
                .map(ByteBuffer::wrap)
                .map(k -> ReactiveStringCommands.SetCommand
                        .set(k)
                        .value(ByteBuffer.wrap(request.getName().getBytes())));

        return connection.stringCommands()
                .set(addCommand)
                .then(Mono.just(new ResponseEntity<Void>(HttpStatus.OK)));
    }

    @GetMapping("/populate")
    public Mono<Void> populate() {
        Flux<String> keyFlux = Flux.range(0, 50).map(i -> ("Key-" + i));

        Flux<ReactiveStringCommands.SetCommand> generator = keyFlux.map(String::getBytes).map(ByteBuffer::wrap)
                .map(key -> ReactiveStringCommands.SetCommand.set(key)
                        .value(ByteBuffer.wrap(UUID.randomUUID().toString().getBytes())));

        connection.stringCommands()
                .set(generator)
                .then()
                .block();
        return Mono.empty();
    }

    private Mono<ByteBuffer> getEntry(String key) {
        return connection.stringCommands().get(ByteBuffer.wrap(serializer.serialize("Key-" + key)));
    }

    private static String toString(ByteBuffer byteBuffer) {
        byte[] bytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytes);
        return new String(bytes);
    }
}
