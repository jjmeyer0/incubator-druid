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

import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.easymock.EasyMock.mock;

public class CaffeineOnHeapLoadingCacheTest
{
  private final LoadingCache<Object, Object> cache = new CaffeineOnHeapLoadingCache<>(1000, 10000L, 10000L, 10000L);

  @Test
  public void testIsClosed()
  {
    CaffeineOnHeapLoadingCache<?, ?> onHeapLookupLoadingCache = new CaffeineOnHeapLoadingCache<>(15, 100L, 10L, 10L);
    onHeapLookupLoadingCache.close();
    Assert.assertTrue(onHeapLookupLoadingCache.isClosed());
  }

  @Test(expected = UncheckedExecutionException.class)
  public void validateUncheckedRethrown() throws Exception
  {
    cache.get("k1", () -> {
      throw new UncheckedExecutionException(mock(Exception.class));
    });
  }

  @Test(expected = UncheckedExecutionException.class)
  public void validateRuntimeThrownAsUnchecked() throws Exception
  {
    cache.get("k1", () -> {
      throw new RuntimeException();
    });
  }

  @Test(expected = ExecutionException.class)
  public void validateExecutionExceptionRethrown() throws Exception
  {
    cache.get("k1", () -> {
      throw new ExecutionException(mock(Exception.class));
    });
  }

  @Test(expected = ExecutionException.class)
  public void validateExceptionThrownAsExecutionException() throws Exception
  {
    cache.get("k1", () -> {
      throw new Exception();
    });
  }

  @Test(expected = ExecutionError.class)
  public void validateErrorRethrownAsExecutionError() throws Exception
  {
    cache.get("k1", () -> {
      throw new Error();
    });
  }
}
