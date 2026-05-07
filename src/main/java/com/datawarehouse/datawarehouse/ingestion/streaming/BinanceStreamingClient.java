package com.datawarehouse.datawarehouse.ingestion.streaming;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketHttpHeaders;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
@Slf4j
public class BinanceStreamingClient {

    private static final String PROVIDER = "Binance";
    private static final String DATA_SOURCE_ID = "BINANCE/SPOT";
    private static final String EVENT_TYPE = "kline";

    private final ObjectMapper objectMapper;
    private final KafkaRawMarketDataProducer rawMarketDataProducer;

    @Value("${streaming.binance.symbols:btcusdt,ethusdt}")
    private String symbols;

    @Value("${streaming.binance.interval:1m}")
    private String interval;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private WebSocketSession session;
    private Instant startedAt;
    private Instant lastMessageAt;
    private String lastError;
    private String lastCloseStatus;

    public synchronized void start() {
        if (running.get()) {
            return;
        }

        try {
            URI uri = URI.create(buildStreamUrl());
            StandardWebSocketClient client = new StandardWebSocketClient();
            lastError = null;
            lastCloseStatus = null;
            log.info("Starting Binance WebSocket stream: {}", uri);

            this.session = client.execute(
                    new BinanceWebSocketHandler(),
                    new WebSocketHttpHeaders(),
                    uri
            ).get();

            running.set(true);
            startedAt = Instant.now();
            log.info("Binance WebSocket stream started. Session open: {}", session.isOpen());
        } catch (Exception exception) {
            running.set(false);
            lastError = exception.getMessage();
            log.error("Failed to start Binance streaming client", exception);
            throw new IllegalStateException("Failed to start Binance streaming client", exception);
        }
    }

    public synchronized void stop() {
        running.set(false);

        if (session != null && session.isOpen()) {
            try {
                session.close();
            } catch (Exception ignored) {
            }
        }

        session = null;
    }

    public boolean isRunning() {
        return running.get() && session != null && session.isOpen();
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public Instant getLastMessageAt() {
        return lastMessageAt;
    }

    public String getLastError() {
        return lastError;
    }

    public String getLastCloseStatus() {
        return lastCloseStatus;
    }

    @PreDestroy
    public void shutdown() {
        stop();
    }

    private String buildStreamUrl() {
        String streams = Arrays.stream(symbols.split(","))
                .map(String::trim)
                .filter(symbol -> !symbol.isBlank())
                .map(String::toLowerCase)
                .map(symbol -> symbol + "@kline_" + interval)
                .collect(Collectors.joining("/"));

        return "wss://stream.binance.com:9443/stream?streams=" + streams;
    }

    private void handleMessage(String payload) {
        try {
            lastMessageAt = Instant.now();
            JsonNode root = objectMapper.readTree(payload);
            JsonNode kline = root.path("data").path("k");

            if (kline.isMissingNode() || kline.isNull()) {
                return;
            }

            String symbol = kline.path("s").textValue();
            Instant eventTime = Instant.ofEpochMilli(root.path("data").path("E").asLong());

            Map<String, Object> values = new LinkedHashMap<>();
            values.put("open", kline.path("o").asDouble());
            values.put("high", kline.path("h").asDouble());
            values.put("low", kline.path("l").asDouble());
            values.put("close", kline.path("c").asDouble());
            values.put("volume", kline.path("v").asDouble());
            values.put("isClosed", kline.path("x").asBoolean());

            RawMarketDataEvent event = new RawMarketDataEvent(
                    PROVIDER,
                    DATA_SOURCE_ID,
                    DATA_SOURCE_ID + "/" + symbol,
                    symbol,
                    EVENT_TYPE,
                    eventTime,
                    Instant.now(),
                    values
            );

            rawMarketDataProducer.publish(event);
            log.info("Published Binance {} event for {} at {}", EVENT_TYPE, symbol, eventTime);
        } catch (Exception exception) {
            lastError = exception.getMessage();
            log.error("Failed to handle Binance WebSocket message", exception);
            throw new IllegalStateException("Failed to handle Binance WebSocket message", exception);
        }
    }

    private class BinanceWebSocketHandler extends TextWebSocketHandler {

        @Override
        public void handleTextMessage(WebSocketSession session, TextMessage message) {
            BinanceStreamingClient.this.handleMessage(message.getPayload());
        }

        @Override
        public void afterConnectionClosed(
                WebSocketSession session,
                CloseStatus status
        ) {
            running.set(false);
            lastCloseStatus = status.toString();
            log.warn("Binance WebSocket stream closed: {}", status);
        }

        @Override
        public void handleTransportError(
                WebSocketSession session,
                Throwable exception
        ) {
            running.set(false);
            lastError = exception.getMessage();
            log.error("Binance WebSocket transport error", exception);
            stop();
        }
    }
}
