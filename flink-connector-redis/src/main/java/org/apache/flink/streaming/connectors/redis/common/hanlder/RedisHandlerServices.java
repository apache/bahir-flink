/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.redis.common;

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
 * @author Ameng
 * Unified class to search for a {@link RedisHanlder} of provided type and properties.
 * for find correct catalog redis.
 * //todo remove table dto;
 */
public class RedisHandlerServices<T> {

	private static final ServiceLoader<RedisHanlder> defaultLoader = ServiceLoader.load(RedisHanlder.class);
	private static final Logger LOG = LoggerFactory.getLogger(RedisHandlerServices.class);

	public static <T extends RedisHanlder> T findRedisHanlder(Class<T> RedisHanlderClass, Map<String, String> meta) {
		Preconditions.checkNotNull(meta);
		return findSingleInternal(RedisHanlderClass, meta, Optional.empty());
	}


	private static <T extends RedisHanlder> T findSingleInternal(
			Class<T> RedisHanlderClass,
			Map<String, String> meta,
			Optional<ClassLoader> classLoader) {

		List<RedisHanlder> RedisHanlders = discoverRedisHanlder(classLoader);
		List<T> filtered = filter(RedisHanlders, RedisHanlderClass, meta);

		return filtered.get(0);
	}


	/**
	 * Filters found redis by factory class and with matching context.
	 */
	private static <T extends RedisHanlder> List<T> filter(
			List<RedisHanlder> redis,
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
	private static List<RedisHanlder> discoverRedisHanlder(Optional<ClassLoader> classLoader) {
		try {
			List<RedisHanlder> result = new LinkedList<>();
			if (classLoader.isPresent()) {
				ServiceLoader
					.load(RedisHanlder.class, classLoader.get())
					.iterator()
					.forEachRemaining(result::add);
			} else {
				defaultLoader.iterator().forEachRemaining(result::add);
			}
			return result;
		} catch (ServiceConfigurationError e) {
			LOG.error("Could not load service provider for catalog redis.", e);
			throw new TableException("Could not load service provider for catalog redis.", e);
		}

	}

	/**
	 * Filters factories with matching context by factory class.
	 */
	@SuppressWarnings("unchecked")
	private static <T> List<T> filterByFactoryClass(
			Class<T> redisClass,
			List<RedisHanlder> redis) {

		List<RedisHanlder> redisList = redis.stream()
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
	private static <T extends RedisHanlder> List<T> filterByContext(
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
	private static Map<String, String> normalizeContext(RedisHanlder redis) {
		Map<String, String> requiredContext = redis.requiredContext();
		if (requiredContext == null) {
			throw new RuntimeException(
				String.format("Required context of redis '%s' must not be null.", redis.getClass().getName()));
		}
		return requiredContext.keySet().stream()
			.collect(Collectors.toMap(String::toLowerCase, requiredContext::get));
	}

}
