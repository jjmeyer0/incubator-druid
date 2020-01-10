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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.ISE;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

@RunWith(Parameterized.class)
public class LoadingCacheTest
{
  private static final ImmutableMap<Object, Object> IMMUTABLE_MAP = ImmutableMap.of("key", "value");
  private final LoadingCache<Object, Object> loadingCache;

  public LoadingCacheTest(LoadingCache<Object, Object> loadingCache)
  {
    this.loadingCache = loadingCache;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> inputData()
  {
    return Arrays.asList(new Object[][]{
        {new OnHeapLoadingCache<>(4, 1000, null, null, null)},
        {new OffHeapLoadingCache<>(0, 0L, 0L, 0L)},
        });
  }

  @Before
  public void setUp()
  {
    Assert.assertFalse(loadingCache.isClosed());
    loadingCache.putAll(IMMUTABLE_MAP);
  }

  @After
  public void tearDown()
  {
    loadingCache.invalidateAll();
  }

  @Test
  public void testGetIfPresent()
  {
    Assert.assertNull(loadingCache.getIfPresent("not there"));
    Assert.assertEquals(IMMUTABLE_MAP.get("key"), loadingCache.getIfPresent("key"));
  }

  @Test
  public void testGetAllPresent()
  {
    Assert.assertEquals(IMMUTABLE_MAP, loadingCache.getAllPresent(IMMUTABLE_MAP.keySet()));
  }

  @Test
  public void testPut()
  {
    loadingCache.get("key2", (k) -> "value2");
    Assert.assertEquals("value2", loadingCache.getIfPresent("key2"));
  }

  @Test
  public void testInvalidate()
  {
    loadingCache.get("key2", (k) -> "value2");
    Assert.assertEquals("value2", loadingCache.getIfPresent("key2"));
    loadingCache.invalidate("key2");
    Assert.assertEquals(null, loadingCache.getIfPresent("key2"));
  }

  @Test
  public void testInvalidateAll()
  {
    loadingCache.get("key2", (k) -> "value2");
    Assert.assertEquals("value2", loadingCache.getIfPresent("key2"));
    loadingCache.invalidateAll(Collections.singletonList("key2"));
    Assert.assertEquals(null, loadingCache.getIfPresent("key2"));
  }

  @Test
  public void testInvalidateAll1()
  {
    loadingCache.invalidateAll();
    loadingCache.get("key2", (k) -> "value2");
    Assert.assertEquals(loadingCache.getAllPresent(IMMUTABLE_MAP.keySet()), Collections.emptyMap());
  }

  @Test
  public void testGetStats()
  {
    Assert.assertTrue(loadingCache.getStats() != null && loadingCache.getStats() instanceof LookupCacheStats);
  }

  @Test
  public void testIsClosed()
  {
    Assert.assertFalse(loadingCache.isClosed());
  }

  @Test
  public void testSerDeser() throws IOException
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    Assert.assertEquals(
        loadingCache,
        mapper.readerFor(LoadingCache.class).readValue(mapper.writeValueAsString(loadingCache))
    );
    Assert.assertEquals(
        loadingCache.hashCode(),
        mapper.readerFor(LoadingCache.class)
              .readValue(mapper.writeValueAsString(loadingCache))
              .hashCode()
    );
  }

  @Test
  public void checkNullFunctionReturnIsntCached()
  {
    final String key = "k1";
    final String value = "v";
    try {
      loadingCache.get(key, (k) -> null);
    }
    catch (final ISE ignore) {
    }

    final Map<Object, Object> m = loadingCache.getAllPresent(Collections.singleton(key));

    Assert.assertTrue(m.isEmpty());
    Assert.assertEquals(value, loadingCache.get(key, (k) -> value));

    final Map<Object, Object> m2 = loadingCache.getAllPresent(Collections.singleton(key));
    Assert.assertFalse(m2.isEmpty());

  }

  @Test(expected = ISE.class)
  public void validateRuntimeThrownAsUnchecked()
  {
    loadingCache.get("k1", (k) -> {
      throw new NullPointerException();
    });
  }

  @Test(expected = ISE.class)
  public void validateExecutionExceptionRethrown()
  {
    loadingCache.get("k1", (k) -> {
      throw new IllegalStateException();
    });
  }

  @Test(expected = ISE.class)
  public void validateExceptionThrownAsExecutionException()
  {
    loadingCache.get("k1", (k) -> {
      throw new RuntimeException();
    });
  }

  @Test(expected = Error.class)
  public void validateErrorRethrownAsExecutionError()
  {
    loadingCache.get("k1", (k) -> {
      throw new Error();
    });
  }
}
