/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.gateway;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.util.EventLoopGroups;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.Consumes;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.JacksonRequestConverterFunction;
import com.linecorp.armeria.server.annotation.Post;
import com.linecorp.armeria.server.annotation.Produces;
import com.linecorp.armeria.server.annotation.RequestConverter;
import com.linecorp.armeria.server.grpc.GrpcService;
import io.camunda.identity.sdk.IdentityConfiguration;
import io.camunda.zeebe.gateway.health.GatewayHealthManager;
import io.camunda.zeebe.gateway.health.Status;
import io.camunda.zeebe.gateway.health.impl.GatewayHealthManagerImpl;
import io.camunda.zeebe.gateway.impl.broker.BrokerClient;
import io.camunda.zeebe.gateway.impl.configuration.AuthenticationCfg.AuthMode;
import io.camunda.zeebe.gateway.impl.configuration.GatewayCfg;
import io.camunda.zeebe.gateway.impl.configuration.IdentityCfg;
import io.camunda.zeebe.gateway.impl.configuration.MultiTenancyCfg;
import io.camunda.zeebe.gateway.impl.configuration.NetworkCfg;
import io.camunda.zeebe.gateway.impl.configuration.SecurityCfg;
import io.camunda.zeebe.gateway.impl.job.ActivateJobsHandler;
import io.camunda.zeebe.gateway.impl.job.LongPollingActivateJobsHandler;
import io.camunda.zeebe.gateway.impl.job.RoundRobinActivateJobsHandler;
import io.camunda.zeebe.gateway.impl.stream.StreamJobsHandler;
import io.camunda.zeebe.gateway.interceptors.impl.ContextInjectingInterceptor;
import io.camunda.zeebe.gateway.interceptors.impl.DecoratedInterceptor;
import io.camunda.zeebe.gateway.interceptors.impl.IdentityInterceptor;
import io.camunda.zeebe.gateway.interceptors.impl.InterceptorRepository;
import io.camunda.zeebe.gateway.query.impl.QueryApiImpl;
import io.camunda.zeebe.protocol.impl.stream.job.JobActivationProperties;
import io.camunda.zeebe.scheduler.Actor;
import io.camunda.zeebe.scheduler.ActorSchedulingService;
import io.camunda.zeebe.scheduler.future.ActorFuture;
import io.camunda.zeebe.scheduler.future.CompletableActorFuture;
import io.camunda.zeebe.transport.stream.api.ClientStreamer;
import io.camunda.zeebe.util.CloseableSilently;
import io.grpc.BindableService;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import me.dinowernli.grpc.prometheus.Configuration;
import me.dinowernli.grpc.prometheus.MonitoringServerInterceptor;
import org.slf4j.Logger;

public final class Gateway implements CloseableSilently {
  private static final Logger LOG = Loggers.GATEWAY_LOGGER;
  private static final MonitoringServerInterceptor MONITORING_SERVER_INTERCEPTOR =
      MonitoringServerInterceptor.create(Configuration.allMetrics());

  private final GatewayCfg gatewayCfg;
  private final IdentityConfiguration identityCfg;
  private final ActorSchedulingService actorSchedulingService;
  private final GatewayHealthManager healthManager;
  private final ClientStreamer<JobActivationProperties> jobStreamer;

  private Server server;
  private ExecutorService grpcExecutor;
  private final BrokerClient brokerClient;

  public Gateway(
      final GatewayCfg gatewayCfg,
      final IdentityConfiguration identityCfg,
      final BrokerClient brokerClient,
      final ActorSchedulingService actorSchedulingService,
      final ClientStreamer<JobActivationProperties> jobStreamer) {
    this.gatewayCfg = gatewayCfg;
    this.identityCfg = identityCfg;
    this.brokerClient = brokerClient;
    this.actorSchedulingService = actorSchedulingService;
    this.jobStreamer = jobStreamer;

    healthManager = new GatewayHealthManagerImpl();
  }

  public Gateway(
      final GatewayCfg gatewayCfg,
      final BrokerClient brokerClient,
      final ActorSchedulingService actorSchedulingService,
      final ClientStreamer<JobActivationProperties> jobStreamer) {
    this(gatewayCfg, null, brokerClient, actorSchedulingService, jobStreamer);
  }

  public GatewayCfg getGatewayCfg() {
    return gatewayCfg;
  }

  public Status getStatus() {
    return healthManager.getStatus();
  }

  public BrokerClient getBrokerClient() {
    return brokerClient;
  }

  public ActorFuture<Gateway> start() {
    final var resultFuture = new CompletableActorFuture<Gateway>();
    healthManager.setStatus(Status.STARTING);

    createAndStartActivateJobsHandler(brokerClient)
        .thenCombine(startClientStreamAdapter(), this::createServer)
        .thenCompose(this::startServer)
        .thenApply(ok -> this)
        .whenComplete(resultFuture);

    return resultFuture;
  }

  private CompletionStage<Void> startServer(final Server server) {
    this.server = server;
    return this.server.start().thenAccept(ok -> healthManager.setStatus(Status.RUNNING));
  }

  private CompletionStage<StreamJobsHandler> startClientStreamAdapter() {
    final var adapter = new StreamJobsHandler(jobStreamer);
    final var future = new CompletableFuture<StreamJobsHandler>();

    actorSchedulingService
        .submitActor(adapter)
        .onComplete(
            (ok, error) -> {
              if (error != null) {
                future.completeExceptionally(error);
                return;
              }

              future.complete(adapter);
            },
            ForkJoinPool.commonPool());

    return future;
  }

  private Server createServer(
      final ActivateJobsHandler activateJobsHandler, final StreamJobsHandler streamJobsHandler) {
    final NetworkCfg network = gatewayCfg.getNetwork();
    final MultiTenancyCfg multiTenancy = gatewayCfg.getMultiTenancy();

    final var serverBuilder = Server.builder();
    applySecurityConfiguration(serverBuilder);
    if (gatewayCfg.getSecurity().isEnabled()) {
      serverBuilder.https(new InetSocketAddress(network.getHost(), network.getPort()));
    } else {
      serverBuilder.http(new InetSocketAddress(network.getHost(), network.getPort()));
    }
    applyExecutorConfiguration(serverBuilder);

    final var endpointManager =
        new EndpointManager(brokerClient, activateJobsHandler, streamJobsHandler, multiTenancy);
    final var gatewayGrpcService = new GatewayGrpcService(endpointManager);
    return buildServer(serverBuilder, gatewayGrpcService);
  }

  private void applyExecutorConfiguration(final ServerBuilder builder) {
    final var config = gatewayCfg.getThreads();
    final var workerGroup = EventLoopGroups.newEventLoopGroup(config.getGrpcMaxThreads());
    grpcExecutor = workerGroup;
    builder.workerGroup(workerGroup, false);
  }

  private void applySecurityConfiguration(final ServerBuilder serverBuilder) {
    final SecurityCfg securityCfg = gatewayCfg.getSecurity();
    if (securityCfg.isEnabled()) {
      setSecurityConfig(serverBuilder, securityCfg);
    }
  }

  private Server buildServer(
      final ServerBuilder serverBuilder, final BindableService interceptorService) {
    final var network = gatewayCfg.getNetwork();
    final var maxMessageSize = (int) network.getMaxMessageSize().toBytes();
    if (maxMessageSize <= 0) {
      throw new IllegalArgumentException("maxMessageSize must be positive");
    }

    final Duration minKeepAliveInterval = network.getMinKeepAliveInterval();
    if (minKeepAliveInterval.isNegative() || minKeepAliveInterval.isZero()) {
      throw new IllegalArgumentException("Minimum keep alive interval must be positive.");
    }

    return serverBuilder
        .service(
            "/failure",
            (ctx, req) -> {
              ctx.setShouldReportUnhandledExceptions(false);
              throw new RuntimeException("Hello Failure!");
            })
        .annotatedService(new HttpService())
        .service(
            GrpcService.builder()
                .useBlockingTaskExecutor(true)
                .addService(applyInterceptors(interceptorService))
                .maxResponseMessageLength(maxMessageSize)
                .build())
        .service(
            GrpcService.builder()
                .addService(
                    ServerInterceptors.intercept(
                        healthManager.getHealthService(), MONITORING_SERVER_INTERCEPTOR))
                .build())
        .build();
  }

  private void setSecurityConfig(final ServerBuilder serverBuilder, final SecurityCfg security) {
    final var certificateChainPath = security.getCertificateChainPath();
    final var privateKeyPath = security.getPrivateKeyPath();

    if (certificateChainPath == null) {
      throw new IllegalArgumentException(
          "Expected to find a valid path to a certificate chain but none was found. "
              + "Edit the gateway configuration file to provide one or to disable TLS.");
    }

    if (privateKeyPath == null) {
      throw new IllegalArgumentException(
          "Expected to find a valid path to a private key but none was found. "
              + "Edit the gateway configuration file to provide one or to disable TLS.");
    }

    if (!certificateChainPath.exists()) {
      throw new IllegalArgumentException(
          String.format(
              "Expected to find a certificate chain file at the provided location '%s' but none was found.",
              certificateChainPath));
    }

    if (!privateKeyPath.exists()) {
      throw new IllegalArgumentException(
          String.format(
              "Expected to find a private key file at the provided location '%s' but none was found.",
              privateKeyPath));
    }

    serverBuilder.tls(certificateChainPath, privateKeyPath);
  }

  @Override
  public void close() {
    healthManager.setStatus(Status.SHUTDOWN);

    if (server != null && !server.isClosed()) {
      server.close();
    }

    if (grpcExecutor != null) {
      grpcExecutor.shutdownNow();
      try {
        //noinspection ResultOfMethodCallIgnored
        grpcExecutor.awaitTermination(10, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        grpcExecutor = null;
      }
    }
  }

  private CompletableFuture<ActivateJobsHandler> createAndStartActivateJobsHandler(
      final BrokerClient brokerClient) {
    final var handler = buildActivateJobsHandler(brokerClient);
    return submitActorToActivateJobs(handler);
  }

  private CompletableFuture<ActivateJobsHandler> submitActorToActivateJobs(
      final ActivateJobsHandler handler) {
    final var future = new CompletableFuture<ActivateJobsHandler>();
    final var actor =
        Actor.newActor()
            .name("ActivateJobsHandler")
            .actorStartedHandler(handler.andThen(t -> future.complete(handler)))
            .build();
    actorSchedulingService.submitActor(actor);
    return future;
  }

  private ActivateJobsHandler buildActivateJobsHandler(final BrokerClient brokerClient) {
    if (gatewayCfg.getLongPolling().isEnabled()) {
      return buildLongPollingHandler(brokerClient);
    } else {
      return new RoundRobinActivateJobsHandler(brokerClient);
    }
  }

  private LongPollingActivateJobsHandler buildLongPollingHandler(final BrokerClient brokerClient) {
    return LongPollingActivateJobsHandler.newBuilder().setBrokerClient(brokerClient).build();
  }

  private ServerServiceDefinition applyInterceptors(final BindableService service) {
    final var repository = new InterceptorRepository().load(gatewayCfg.getInterceptors());
    final var queryApi = new QueryApiImpl(brokerClient);
    final List<ServerInterceptor> interceptors =
        repository.instantiate().map(DecoratedInterceptor::decorate).collect(Collectors.toList());

    // reverse the user interceptors, such that they will be called in the order in which they are
    // configured, such that the first configured interceptor is the outermost interceptor in the
    // chain
    Collections.reverse(interceptors);
    interceptors.add(new ContextInjectingInterceptor(queryApi));
    interceptors.add(MONITORING_SERVER_INTERCEPTOR);
    if (AuthMode.IDENTITY == gatewayCfg.getSecurity().getAuthentication().getMode()) {
      final var zeebeIdentityCfg = gatewayCfg.getSecurity().getAuthentication().getIdentity();
      if (isZeebeIdentityConfigurationNotNull(zeebeIdentityCfg)) {
        interceptors.add(new IdentityInterceptor(zeebeIdentityCfg, gatewayCfg.getMultiTenancy()));
        LOG.warn(
            "These Zeebe configuration properties for Camunda Identity are deprecated! Please use the "
                + "corresponding Camunda Identity properties or the environment variables defined here: "
                + "https://docs.camunda.io/docs/self-managed/identity/deployment/configuration-variables/");
      } else {
        interceptors.add(new IdentityInterceptor(identityCfg, gatewayCfg.getMultiTenancy()));
      }
    }

    return ServerInterceptors.intercept(service, interceptors);
  }

  private boolean isZeebeIdentityConfigurationNotNull(final IdentityCfg identityCfg) {
    return identityCfg.getIssuerBackendUrl() != null || identityCfg.getBaseUrl() != null;
  }

  private static final class HttpService {
    @Produces("application/text")
    @Get("/get")
    public HttpResponse get(final ServiceRequestContext ctx) {
      return HttpResponse.of("Bar");
    }

    @RequestConverter(JacksonRequestConverterFunction.class)
    @Consumes("application/json")
    @Produces("application/text")
    @Post("/post")
    public HttpResponse post(final ServiceRequestContext ctx, final PostRequest request) {
      return HttpResponse.of("Received " + request.field);
    }

    private record PostRequest(int field) {}
  }
}
