package org.hiast.batch.adapter.driver.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.hiast.batch.config.AppConfig;
import org.hiast.batch.config.MongoConfig;
import org.hiast.batch.config.RedisConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * A simple HTTP server that provides health check endpoints for the batch training service.
 * This allows monitoring systems to check if the service is running and if its dependencies
 * are available.
 */
public class HealthCheckServer {
    private static final Logger log = LoggerFactory.getLogger(HealthCheckServer.class);
    private static final int DEFAULT_PORT = 8080;
    private final HttpServer server;
    private final AppConfig appConfig;

    /**
     * Creates a new health check server.
     *
     * @param port The port to listen on
     * @param appConfig The application configuration
     * @throws IOException If the server cannot be created
     */
    public HealthCheckServer(int port, AppConfig appConfig) throws IOException {
        this.appConfig = appConfig;
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/health", new HealthCheckHandler());
        server.createContext("/readiness", new ReadinessCheckHandler());
        server.setExecutor(Executors.newFixedThreadPool(2));
        log.info("Health check server created on port {}", port);
    }

    /**
     * Creates a new health check server on the default port.
     *
     * @param appConfig The application configuration
     * @throws IOException If the server cannot be created
     */
    public HealthCheckServer(AppConfig appConfig) throws IOException {
        this(DEFAULT_PORT, appConfig);
    }

    /**
     * Starts the health check server.
     */
    public void start() {
        server.start();
        log.info("Health check server started on port {}", server.getAddress().getPort());
    }

    /**
     * Stops the health check server.
     */
    public void stop() {
        server.stop(0);
        log.info("Health check server stopped");
    }

    /**
     * Handler for the /health endpoint.
     * This endpoint checks if the service is running.
     */
    private class HealthCheckHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            log.debug("Received health check request from {}", exchange.getRemoteAddress());
            String response = "{\"status\":\"UP\"}";
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.length());
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes());
            }
        }
    }

    /**
     * Handler for the /readiness endpoint.
     * This endpoint checks if the service and its dependencies are ready to handle requests.
     */
    private class ReadinessCheckHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            log.debug("Received readiness check request from {}", exchange.getRemoteAddress());
            
            Map<String, String> checks = new HashMap<>();
            checks.put("service", "UP");
            
            // Check Redis connection
            RedisConfig redisConfig = appConfig.getRedisConfig();
            try (Jedis jedis = new Jedis(redisConfig.getHost(), redisConfig.getPort())) {
                jedis.ping();
                checks.put("redis", "UP");
            } catch (JedisConnectionException e) {
                log.warn("Redis connection check failed: {}", e.getMessage());
                checks.put("redis", "DOWN");
            }
            
            // Check MongoDB connection
            MongoConfig mongoConfig = appConfig.getMongoConfig();
            try {
                // We're not using a full MongoDB client here to avoid heavy dependencies
                // Just check if we can establish a socket connection
                new java.net.Socket(mongoConfig.getHost(), mongoConfig.getPort()).close();
                checks.put("mongodb", "UP");
            } catch (IOException e) {
                log.warn("MongoDB connection check failed: {}", e.getMessage());
                checks.put("mongodb", "DOWN");
            }
            
            // Build response
            StringBuilder responseBuilder = new StringBuilder();
            responseBuilder.append("{");
            responseBuilder.append("\"status\":\"").append(checks.containsValue("DOWN") ? "DOWN" : "UP").append("\",");
            responseBuilder.append("\"checks\":{");
            
            int i = 0;
            for (Map.Entry<String, String> entry : checks.entrySet()) {
                if (i > 0) {
                    responseBuilder.append(",");
                }
                responseBuilder.append("\"").append(entry.getKey()).append("\":\"").append(entry.getValue()).append("\"");
                i++;
            }
            
            responseBuilder.append("}}");
            
            String response = responseBuilder.toString();
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(checks.containsValue("DOWN") ? 503 : 200, response.length());
            
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes());
            }
        }
    }
}
