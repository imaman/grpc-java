/*
 * Copyright 2016, gRPC Authors All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.grpc.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.Attributes;
import io.grpc.CompressorRegistry;
import io.grpc.DecompressorRegistry;
import io.grpc.LoadBalancer;
import io.grpc.NameResolver;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link AbstractManagedChannelImplBuilder}. */
@RunWith(JUnit4.class)
public class AbstractManagedChannelImplBuilderTest {
  private Builder builder = new Builder("fake");
  private Builder directAddressBuilder = new Builder(new SocketAddress(){}, "fake");

  @Test
  public void executor_default() {
    assertNotNull(builder.executorPool);
  }

  @Test
  public void executor_normal() {
    Executor executor = mock(Executor.class);
    assertEquals(builder, builder.executor(executor));
    assertEquals(executor, builder.executorPool.getObject());
  }

  @Test
  public void executor_null() {
    ObjectPool<? extends Executor> defaultValue = builder.executorPool;
    builder.executor(mock(Executor.class));
    assertEquals(builder, builder.executor(null));
    assertEquals(defaultValue, builder.executorPool);
  }

  @Test
  public void directExecutor() {
    assertEquals(builder, builder.directExecutor());
    assertEquals(MoreExecutors.directExecutor(), builder.executorPool.getObject());
  }

  @Test
  public void nameResolverFactory_default() {
    assertNotNull(builder.nameResolverFactory);
  }

  @Test
  public void nameResolverFactory_normal() {
    NameResolver.Factory nameResolverFactory = mock(NameResolver.Factory.class);
    assertEquals(builder, builder.nameResolverFactory(nameResolverFactory));
    assertEquals(nameResolverFactory, builder.nameResolverFactory);
  }

  @Test
  public void nameResolverFactory_null() {
    NameResolver.Factory defaultValue = builder.nameResolverFactory;
    builder.nameResolverFactory(mock(NameResolver.Factory.class));
    assertEquals(builder, builder.nameResolverFactory(null));
    assertEquals(defaultValue, builder.nameResolverFactory);
  }

  @Test(expected = IllegalStateException.class)
  public void nameResolverFactory_notAllowedWithDirectAddress() {
    directAddressBuilder.nameResolverFactory(mock(NameResolver.Factory.class));
  }

  @Test
  public void loadBalancerFactory_default() {
    assertNotNull(builder.loadBalancerFactory);
  }

  @Test
  public void loadBalancerFactory_normal() {
    LoadBalancer.Factory loadBalancerFactory = mock(LoadBalancer.Factory.class);
    assertEquals(builder, builder.loadBalancerFactory(loadBalancerFactory));
    assertEquals(loadBalancerFactory, builder.loadBalancerFactory);
  }

  @Test
  public void loadBalancerFactory_null() {
    LoadBalancer.Factory defaultValue = builder.loadBalancerFactory;
    builder.loadBalancerFactory(mock(LoadBalancer.Factory.class));
    assertEquals(builder, builder.loadBalancerFactory(null));
    assertEquals(defaultValue, builder.loadBalancerFactory);
  }

  @Test(expected = IllegalStateException.class)
  public void loadBalancerFactory_notAllowedWithDirectAddress() {
    directAddressBuilder.loadBalancerFactory(mock(LoadBalancer.Factory.class));
  }

  @Test
  public void decompressorRegistry_default() {
    assertNotNull(builder.decompressorRegistry);
  }

  @Test
  public void decompressorRegistry_normal() {
    DecompressorRegistry decompressorRegistry = DecompressorRegistry.emptyInstance();
    assertNotEquals(decompressorRegistry, builder.decompressorRegistry);
    assertEquals(builder, builder.decompressorRegistry(decompressorRegistry));
    assertEquals(decompressorRegistry, builder.decompressorRegistry);
  }

  @Test
  public void decompressorRegistry_null() {
    DecompressorRegistry defaultValue = builder.decompressorRegistry;
    assertEquals(builder, builder.decompressorRegistry(DecompressorRegistry.emptyInstance()));
    assertNotEquals(defaultValue, builder.decompressorRegistry);
    builder.decompressorRegistry(null);
    assertEquals(defaultValue, builder.decompressorRegistry);
  }

  @Test
  public void compressorRegistry_default() {
    assertNotNull(builder.compressorRegistry);
  }

  @Test
  public void compressorRegistry_normal() {
    CompressorRegistry compressorRegistry = CompressorRegistry.newEmptyInstance();
    assertNotEquals(compressorRegistry, builder.compressorRegistry);
    assertEquals(builder, builder.compressorRegistry(compressorRegistry));
    assertEquals(compressorRegistry, builder.compressorRegistry);
  }

  @Test
  public void compressorRegistry_null() {
    CompressorRegistry defaultValue = builder.compressorRegistry;
    builder.compressorRegistry(CompressorRegistry.newEmptyInstance());
    assertNotEquals(defaultValue, builder.compressorRegistry);
    assertEquals(builder, builder.compressorRegistry(null));
    assertEquals(defaultValue, builder.compressorRegistry);
  }

  @Test
  public void userAgent_default() {
    assertNull(builder.userAgent);
  }

  @Test
  public void userAgent_normal() {
    String userAgent = "user-agent/1";
    assertEquals(builder, builder.userAgent(userAgent));
    assertEquals(userAgent, builder.userAgent);
  }

  @Test
  public void userAgent_null() {
    assertEquals(builder, builder.userAgent(null));
    assertNull(builder.userAgent);

    builder.userAgent("user-agent/1");
    builder.userAgent(null);
    assertNull(builder.userAgent);
  }

  @Test
  public void overrideAuthority_default() {
    assertNull(builder.authorityOverride);
  }

  @Test
  public void overrideAuthority_normal() {
    String overrideAuthority = "best-authority";
    assertEquals(builder, builder.overrideAuthority(overrideAuthority));
    assertEquals(overrideAuthority, builder.authorityOverride);
  }

  @Test(expected = NullPointerException.class)
  public void overrideAuthority_null() {
    builder.overrideAuthority(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void overrideAuthority_invalid() {
    builder.overrideAuthority("not_allowed");
  }

  @Test
  public void makeTargetStringForDirectAddress_scopedIpv6() throws Exception {
    InetSocketAddress address = new InetSocketAddress("0:0:0:0:0:0:0:0%0", 10005);
    assertEquals("/0:0:0:0:0:0:0:0%0:10005", address.toString());
    String target = AbstractManagedChannelImplBuilder.makeTargetStringForDirectAddress(address);
    URI uri = new URI(target);
    assertEquals("directaddress:////0:0:0:0:0:0:0:0%250:10005", target);
    assertEquals(target, uri.toString());
  }

  @Test
  public void idleTimeout() {
    Builder builder = new Builder("target");

    assertEquals(AbstractManagedChannelImplBuilder.IDLE_MODE_DEFAULT_TIMEOUT_MILLIS,
        builder.getIdleTimeoutMillis());

    builder.idleTimeout(Long.MAX_VALUE, TimeUnit.DAYS);
    assertEquals(ManagedChannelImpl.IDLE_TIMEOUT_MILLIS_DISABLE, builder.getIdleTimeoutMillis());

    builder.idleTimeout(AbstractManagedChannelImplBuilder.IDLE_MODE_MAX_TIMEOUT_DAYS,
        TimeUnit.DAYS);
    assertEquals(ManagedChannelImpl.IDLE_TIMEOUT_MILLIS_DISABLE, builder.getIdleTimeoutMillis());

    try {
      builder.idleTimeout(0, TimeUnit.SECONDS);
      fail("Should throw");
    } catch (IllegalArgumentException e) {
      // expected
    }

    builder.idleTimeout(1, TimeUnit.NANOSECONDS);
    assertEquals(AbstractManagedChannelImplBuilder.IDLE_MODE_MIN_TIMEOUT_MILLIS,
        builder.getIdleTimeoutMillis());

    builder.idleTimeout(30, TimeUnit.SECONDS);
    assertEquals(TimeUnit.SECONDS.toMillis(30), builder.getIdleTimeoutMillis());
  }

  @Test
  public void overrideAuthorityNameResolverWrapsDelegateTest() {
    NameResolver nameResolverMock = mock(NameResolver.class);
    NameResolver.Factory wrappedFactory = mock(NameResolver.Factory.class);
    when(wrappedFactory.newNameResolver(any(URI.class), any(Attributes.class)))
      .thenReturn(nameResolverMock);
    String override = "override:5678";
    NameResolver.Factory factory =
        new OverrideAuthorityNameResolverFactory(wrappedFactory, override);
    NameResolver nameResolver = factory.newNameResolver(URI.create("dns:///localhost:443"),
        Attributes.EMPTY);
    assertNotNull(nameResolver);
    assertEquals(override, nameResolver.getServiceAuthority());
  }

  @Test
  public void overrideAuthorityNameResolverWontWrapNullTest() {
    NameResolver.Factory wrappedFactory = mock(NameResolver.Factory.class);
    when(wrappedFactory.newNameResolver(any(URI.class), any(Attributes.class))).thenReturn(null);
    NameResolver.Factory factory =
        new OverrideAuthorityNameResolverFactory(wrappedFactory, "override:5678");
    assertEquals(null,
        factory.newNameResolver(URI.create("dns:///localhost:443"), Attributes.EMPTY));
  }

  static class Builder extends AbstractManagedChannelImplBuilder<Builder> {
    Builder(String target) {
      super(target);
    }

    Builder(SocketAddress directServerAddress, String authority) {
      super(directServerAddress, authority);
    }

    @Override
    protected ClientTransportFactory buildTransportFactory() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder usePlaintext(boolean value) {
      throw new UnsupportedOperationException();
    }
  }
}
