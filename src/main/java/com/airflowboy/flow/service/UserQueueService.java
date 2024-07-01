package com.airflowboy.flow.service;

import com.airflowboy.flow.exception.ErrorCode;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;

import static com.airflowboy.flow.exception.ErrorCode.QUEUE_ALREADY_REGISTERED_USER;

@Service
@RequiredArgsConstructor
public class UserQueueService {

    private final ReactiveRedisTemplate<String, String> reactiveRedisTemplate;

    private final String USER_QUEUE_PREFIX = "user:queue:%s:wait";

    //대기열 등록 API
    public Mono<Long> registerWaitQueue(final String queue, final Long userId) {
        // redis : sortedSet
        // key : userId
        // value : unix timestamp
        // rank 표시
        var unixTimestamp = Instant.now().getEpochSecond();
        return reactiveRedisTemplate.opsForZSet().add(USER_QUEUE_PREFIX.formatted(queue), userId.toString(), unixTimestamp)
                .filter(i -> i)
                .switchIfEmpty(Mono.error(ErrorCode.QUEUE_ALREADY_REGISTERED_USER.build()))
                .flatMap(i -> reactiveRedisTemplate.opsForZSet().rank(USER_QUEUE_PREFIX.formatted(queue), userId.toString()))
                .map(i -> i >= 0 ? i+1 : i);
    }
}
