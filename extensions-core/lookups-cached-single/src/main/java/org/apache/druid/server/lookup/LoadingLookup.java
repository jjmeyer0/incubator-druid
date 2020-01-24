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

package org.apache.druid.server.lookup;


import com.google.common.base.Preconditions;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.server.lookup.cache.loading.LoadingCache;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * Loading lookup will load the key/value pair upon request on the key it self, the general algorithm is load key if
 * absent. Once the key/value pair is loaded, eviction will occur according to the cache eviction policy.
 * This module comes with two loading cache implementations, the first, {@link org.apache.druid.server.lookup.cache.loading.OnHeapLoadingCache},
 * is an on-heap cache backed by a Caffeine cache implementation, the second, {@link org.apache.druid.server.lookup.cache.loading.OffHeapLoadingCache},
 * is a MapDB off-heap implementation. Both implementations offer various eviction strategies.
 */
public class LoadingLookup extends LookupExtractor
{
  private static final Logger LOGGER = new Logger(LoadingLookup.class);

  private final DataFetcher<String, String> dataFetcher;
  private final LoadingCache<String, String> loadingCache;
  private final LoadingCache<String, List<String>> reverseLoadingCache;
  private final AtomicBoolean isOpen;
  private final String id = Integer.toHexString(System.identityHashCode(this));

  public LoadingLookup(
      DataFetcher dataFetcher,
      LoadingCache<String, String> loadingCache,
      LoadingCache<String, List<String>> reverseLoadingCache
  )
  {
    this.dataFetcher = Preconditions.checkNotNull(dataFetcher, "lookup must have a DataFetcher");
    this.loadingCache = Preconditions.checkNotNull(loadingCache, "loading lookup need a cache");
    this.reverseLoadingCache = Preconditions.checkNotNull(reverseLoadingCache, "loading lookup need reverse cache");
    this.isOpen = new AtomicBoolean(true);
  }


  @Override
  public String apply(@Nullable final String key)
  {
    String keyEquivalent = NullHandling.nullToEmptyIfNeeded(key);
    if (keyEquivalent == null) {
      // valueEquivalent is null only for SQL Compatible Null Behavior
      // otherwise null will be replaced with empty string in nullToEmptyIfNeeded above.
      return null;
    }

    final String presentVal;
    try {
      presentVal = loadingCache.get(keyEquivalent, new ApplyFunction());
      return NullHandling.emptyToNullIfNeeded(presentVal);
    }
    // This changes functionality. We will now handle all exceptions (except NPE) from both guava and mapdb.
    catch (final ISE e) {
      LOGGER.debug("value not found for key [%s]", key);
      return null;
    }
  }

  @Override
  public List<String> unapply(@Nullable final String value)
  {
    String valueEquivalent = NullHandling.nullToEmptyIfNeeded(value);
    if (valueEquivalent == null) {
      // valueEquivalent is null only for SQL Compatible Null Behavior
      // otherwise null will be replaced with empty string in nullToEmptyIfNeeded above.
      // null value maps to empty list when SQL Compatible
      return Collections.emptyList();
    }
    final List<String> retList;
    try {
      retList = reverseLoadingCache.get(valueEquivalent, new UnapplyFunction());
      return retList;
    }
    // This changes functionality. We will now handle all exceptions (except NPE) from both guava and mapdb.
    catch (final ISE e) {
      LOGGER.debug("list of keys not found for value [%s]", value);
      return Collections.emptyList();
    }
  }

  @Override
  public boolean canIterate()
  {
    return false;
  }

  @Override
  public Iterable<Map.Entry<String, String>> iterable()
  {
    throw new UnsupportedOperationException("Cannot iterate");
  }

  @Override
  public byte[] getCacheKey()
  {
    return LookupExtractionModule.getRandomCacheKey();
  }

  @Override
  public String toString()
  {
    return "LoadingLookup{" +
           "dataFetcher=" + dataFetcher +
           ", id='" + id + '\'' +
           '}';
  }

  private class ApplyFunction implements UnaryOperator<String>
  {
    @Override
    public String apply(final String key)
    {
      // When SQL compatible null handling is disabled,
      // avoid returning null and return an empty string to cache it.
      return NullHandling.nullToEmptyIfNeeded(dataFetcher.fetch(key));
    }
  }

  private class UnapplyFunction implements Function<String, List<String>>
  {

    @Override
    public List<String> apply(final String key)
    {
      return dataFetcher.reverseFetchKeys(key);
    }
  }
}
