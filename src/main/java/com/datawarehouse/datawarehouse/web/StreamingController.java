package com.datawarehouse.datawarehouse.web;

import com.datawarehouse.datawarehouse.ingestion.streaming.BinanceStreamingClient;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.Map;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/streaming/binance")
public class StreamingController {

    private final BinanceStreamingClient binanceStreamingClient;

    @PostMapping("/start")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Map<String, Object> start() {
        binanceStreamingClient.start();

        return Map.of(
                "provider", "Binance",
                "status", "RUNNING",
                "message", "Binance streaming client started",
                "startedAt", Instant.now()
        );
    }

    @PostMapping("/stop")
    public Map<String, Object> stop() {
        binanceStreamingClient.stop();

        return Map.of(
                "provider", "Binance",
                "status", "STOPPED",
                "message", "Binance streaming client stopped",
                "stoppedAt", Instant.now()
        );
    }

    @GetMapping("/status")
    public Map<String, Object> status() {
        boolean running = binanceStreamingClient.isRunning();

        return new java.util.LinkedHashMap<>(Map.of(
                "provider", "Binance",
                "status", running ? "RUNNING" : "STOPPED",
                "running", running,
                "checkedAt", Instant.now()
        )) {{
            put("startedAt", binanceStreamingClient.getStartedAt());
            put("lastMessageAt", binanceStreamingClient.getLastMessageAt());
            put("lastCloseStatus", binanceStreamingClient.getLastCloseStatus());
            put("lastError", binanceStreamingClient.getLastError());
        }};
    }
}
