/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.server.lookup.cache.loading;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.Map;
import java.util.function.Function;

/**
 * A semi-persistent mapping from keys to values. Cache entries are added using
 * {@link #get(Object, Function)} and stored in the cache until either evicted or manually invalidated.
 * <p>
 * <p>Implementations of this interface are expected to be thread-safe, and can be safely accessed
 * by multiple concurrent threads.
 * <p>
 * This interface borrows ideas (and in some cases methods and javadoc) from Guava, Caffeine, and JCache cache interface.
 * Thanks Guava, Caffeine, and JSR!
 * We elected to make this as close as possible to JSR API like that users can build bridges between all the existing implementations of JSR.
 */

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "guava", value = OnHeapLoadingCache.class),
    @JsonSubTypes.Type(name = "mapDb", value = OffHeapLoadingCache.class)
})
public interface LoadingCache<K, V> extends Closeable
{
  /**
   * @param key must not be null
   * @return the value associated with {@code key} in this cache, or {@code null} if there is no
   * cached value for {@code key}.
   * a cache miss should be counted if the key is missing.
   */
  @Nullable
  V getIfPresent(K key);

  /**
   * Returns a map of the values associated with {@code keys} in this cache. The returned map will
   * only contain entries which are already present in the cache.
   */
  Map<K, V> getAllPresent(Iterable<K> keys);

  /**
   * Returns the value associated with the {@code key} in this cache, obtaining that value from the
   * {@code valueLoader} if necessary. This method provides a simple substitute for the
   * conventional "if cached, return; otherwise create, cache and return" pattern.
   * <p>
   * If the specified key is not already associated with a value, attempts to compute its value
   * using the given mapping function and enters it into this cache unless {@code null}. The entire
   * method invocation is performed atomically, so the function is applied at most once per key.
   * Some attempted update operations on this cache by other threads may be blocked while the
   * computation is in progress, so the computation should be short and simple, and must not attempt
   * to update any other mappings of this cache.
   * <p>
   * <b>Warning:</b> {@code valueLoader} <b>must not</b> attempt to update any other mappings of this cache.
   *
   * @throws ISE                  if an issue occurs when getting/loading a value from the cache, or if the value retrieved is null.
   * @throws NullPointerException if {@code key} or {@code valueLoader} is null.
   */
  V get(K key, Function<? super K, ? extends V> valueLoader);

  /**
   * Copies all of the mappings from the specified map to the cache. This method is used for bulk put.
   *
   * @throws IllegalStateException if the cache is {@link #isClosed()}
   */

  void putAll(Map<? extends K, ? extends V> m);

  /**
   * Discards any cached value for key {@code key}. Eviction can be lazy or eager.
   *
   * @throws IllegalStateException if the cache is {@link #isClosed()}
   */
  void invalidate(K key);

  /**
   * Discards any cached values for keys {@code keys}. Eviction can be lazy or eager.
   *
   * @throws IllegalStateException if the cache is {@link #isClosed()}
   */
  void invalidateAll(Iterable<K> keys);

  /**
   * Clears the contents of the cache.
   *
   * @throws IllegalStateException if the cache is {@link #isClosed()}
   */
  void invalidateAll();

  /**
   * @return Stats of the cache.
   * @throws IllegalStateException if the cache is {@link #isClosed()}
   */
  LookupCacheStats getStats();

  /**
   * @return true if the Cache is closed
   */
  boolean isClosed();

  /**
   * Clean the used resources of the cache. Still not sure about cache lifecycle but as an initial design
   * the namespace deletion event should call this method to clean up resources.
   */
  @Override
  void close();
}
