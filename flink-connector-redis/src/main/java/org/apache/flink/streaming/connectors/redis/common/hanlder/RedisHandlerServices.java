/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.redis.common.hanlder;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import org.apache.flink.table.api.TableException;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unified class to search for a {@link RedisHandler} of provided type and properties.
 * for find correct redis handler.
 * @param <T> redis handler type.
 */
public class RedisHandlerServices<T> {

    private static final ServiceLoader<RedisHandler> defaultLoader = ServiceLoader.load(RedisHandler.class);
    private static final Logger LOG = LoggerFactory.getLogger(RedisHandlerServices.class);

    /**
     * use specified class and properties to find redis handler.
     * @param RedisHanlderClass specified redis handler class.
     * @param meta properties to search redis handler
     * @param <T>
     * @return
     */
    public static <T extends RedisHandler> T findRedisHandler(Class<T> RedisHanlderClass, Map<String, String> meta) {
        Preconditions.checkNotNull(meta);
        return findSingRedisHandler(RedisHanlderClass, meta, Optional.empty());
    }


    /**
     * use specified class and properties and class loader to find redis handler.
     * @param RedisHanlderClass specified redis handler class.
     * @param meta properties to search redis handler
     * @param classLoader class loader to load redis handler class
     * @param <T> redis handler
     * @return matched redis handler
     */
    private static <T extends RedisHandler> T findSingRedisHandler(
            Class<T> RedisHanlderClass,
            Map<String, String> meta,
            Optional<ClassLoader> classLoader) {

        List<RedisHandler> redisHandlers = discoverRedisHanlder(classLoader);
        List<T> filtered = filter(redisHandlers, RedisHanlderClass, meta);

        return filtered.get(0);
    }


    /**
     * Filters found redis by factory class and with matching context.
     */
    private static <T extends RedisHandler> List<T> filter(
            List<RedisHandler> redis,
            Class<T> redisClass,
            Map<String, String> meta) {

        Preconditions.checkNotNull(redisClass);
        Preconditions.checkNotNull(meta);

        List<T> redisFactories = filterByFactoryClass(
                redisClass,
                redis);

        List<T> contextFactories = filterByContext(
                meta,
                redisFactories);
        return contextFactories;
    }

    /**
     * Searches for redis using Java service providers.
     *
     * @return all redis in the classpath
     */
    private static List<RedisHandler> discoverRedisHanlder(Optional<ClassLoader> classLoader) {
        try {
            List<RedisHandler> result = new LinkedList<>();
            if (classLoader.isPresent()) {
                ServiceLoader
                        .load(RedisHandler.class, classLoader.get())
                        .iterator()
                        .forEachRemaining(result::add);
            } else {
                defaultLoader.iterator().forEachRemaining(result::add);
            }
            return result;
        } catch (ServiceConfigurationError e) {
            LOG.error("Could not load service provider for redis handler.", e);
            throw new TableException("Could not load service provider for redis handler.", e);
        }

    }

    /**
     * Filters factories with matching context by factory class.
     */
    @SuppressWarnings("unchecked")
    private static <T> List<T> filterByFactoryClass(
            Class<T> redisClass,
            List<RedisHandler> redis) {

        List<RedisHandler> redisList = redis.stream()
                .filter(p -> redisClass.isAssignableFrom(p.getClass()))
                .collect(Collectors.toList());

        if (redisList.isEmpty()) {
            throw new RuntimeException(
                    String.format("No redis hanlder implements '%s'.", redisClass.getCanonicalName()));
        }

        return (List<T>) redisList;
    }

    /**
     * Filters for factories with matching context.
     *
     * @return all matching factories
     */
    private static <T extends RedisHandler> List<T> filterByContext(
            Map<String, String> meta,
            List<T> redisList) {

        List<T> matchingredis = redisList.stream().filter(factory -> {
            Map<String, String> requestedContext = normalizeContext(factory);

            Map<String, String> plainContext = new HashMap<>(requestedContext);

            // check if required context is met
            return plainContext.keySet()
                    .stream()
                    .allMatch(e -> meta.containsKey(e) && meta.get(e).equals(plainContext.get(e)));
        }).collect(Collectors.toList());

        if (matchingredis.isEmpty()) {
            throw new RuntimeException("no match redis");
        }

        return matchingredis;
    }

    /**
     * Prepares the properties of a context to be used for match operations.
     */
    private static Map<String, String> normalizeContext(RedisHandler redis) {
        Map<String, String> requiredContext = redis.requiredContext();
        if (requiredContext == null) {
            throw new RuntimeException(
                    String.format("Required context of redis '%s' must not be null.", redis.getClass().getName()));
        }
        return requiredContext.keySet().stream()
                .collect(Collectors.toMap(String::toLowerCase, requiredContext::get));
    }

}
