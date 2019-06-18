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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.entrypoint.ClusterConfiguration;
import org.apache.flink.runtime.entrypoint.ClusterConfigurationParserFactory;
import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalException;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.util.MetricUtils;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.taskmanager.MemoryLogger;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;

import akka.actor.ActorSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class is the executable entry point for the task manager in yarn or standalone mode. //TaskManager standalone/yarn模式的启动类
 * It constructs the related components (network, I/O manager, memory manager, RPC service, HA service) //这里构建并启动了一些组件，有 I/O、内存管理、RPC、HA
 * and starts them.
 */
public class TaskManagerRunner implements FatalErrorHandler, AutoCloseableAsync {

	private static final Logger LOG = LoggerFactory.getLogger(TaskManagerRunner.class);

	private static final long FATAL_ERROR_SHUTDOWN_TIMEOUT_MS = 10000L;

	private static final int STARTUP_FAILURE_RETURN_CODE = 1;

	public static final int RUNTIME_FAILURE_RETURN_CODE = 2;

	private final Object lock = new Object();

	private final Configuration configuration;

	private final ResourceID resourceId;

	private final Time timeout;

	private final RpcService rpcService;

	private final ActorSystem metricQueryServiceActorSystem;

	private final HighAvailabilityServices highAvailabilityServices;

	private final MetricRegistryImpl metricRegistry;

	private final BlobCacheService blobCacheService;

	/** Executor used to run future callbacks. */
	private final ExecutorService executor;

	private final TaskExecutor taskManager;

	private final CompletableFuture<Void> terminationFuture;

	private boolean shutdown;

	public TaskManagerRunner(Configuration configuration, ResourceID resourceId) throws Exception {
		this.configuration = checkNotNull(configuration);
		this.resourceId = checkNotNull(resourceId);

		timeout = AkkaUtils.getTimeoutAsTime(configuration); //超时时间

		this.executor = java.util.concurrent.Executors.newScheduledThreadPool( //生成可调度的线程池
			Hardware.getNumberCPUCores(), //线程池的线程数量是硬件的CPU核数
			new ExecutorThreadFactory("taskmanager-future"));

		highAvailabilityServices = HighAvailabilityServicesUtils.createHighAvailabilityServices( //创建高可用的服务（基本都是基于zk的）
			configuration,
			executor,
			HighAvailabilityServicesUtils.AddressResolution.TRY_ADDRESS_RESOLUTION);

		rpcService = createRpcService(configuration, highAvailabilityServices); //创建rpc服务
		metricQueryServiceActorSystem = MetricUtils.startMetricsActorSystem(configuration, rpcService.getAddress(), LOG); //Metrics相关

		HeartbeatServices heartbeatServices = HeartbeatServices.fromConfiguration(configuration);//心跳service

		metricRegistry = new MetricRegistryImpl(MetricRegistryConfiguration.fromConfiguration(configuration));

		// TODO: Temporary hack until the MetricQueryService has been ported to RpcEndpoint
		metricRegistry.startQueryService(metricQueryServiceActorSystem, resourceId);

		blobCacheService = new BlobCacheService(
			configuration, highAvailabilityServices.createBlobStore(), null
		);

		taskManager = startTaskManager( //通过上诉的一些配置和service，启动taskmanager //enter
			this.configuration,
			this.resourceId,
			rpcService,
			highAvailabilityServices,
			heartbeatServices,
			metricRegistry,
			blobCacheService,
			false,
			this);

		this.terminationFuture = new CompletableFuture<>();
		this.shutdown = false;

		MemoryLogger.startIfConfigured(LOG, configuration, metricQueryServiceActorSystem);
	}

	// --------------------------------------------------------------------------------------------
	//  Lifecycle management
	// --------------------------------------------------------------------------------------------

	public void start() throws Exception {
		taskManager.start();
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		synchronized (lock) {
			if (!shutdown) {
				shutdown = true;

				final CompletableFuture<Void> taskManagerTerminationFuture = taskManager.closeAsync();

				final CompletableFuture<Void> serviceTerminationFuture = FutureUtils.composeAfterwards(
					taskManagerTerminationFuture,
					this::shutDownServices);

				serviceTerminationFuture.whenComplete(
					(Void ignored, Throwable throwable) -> {
						if (throwable != null) {
							terminationFuture.completeExceptionally(throwable);
						} else {
							terminationFuture.complete(null);
						}
					});
			}
		}

		return terminationFuture;
	}

	private CompletableFuture<Void> shutDownServices() {
		synchronized (lock) {
			Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(3);
			Exception exception = null;

			try {
				blobCacheService.close();
			} catch (Exception e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}

			try {
				metricRegistry.shutdown();
			} catch (Exception e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}

			if (metricQueryServiceActorSystem != null) {
				terminationFutures.add(AkkaUtils.terminateActorSystem(metricQueryServiceActorSystem));
			}

			try {
				highAvailabilityServices.close();
			} catch (Exception e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}

			terminationFutures.add(rpcService.stopService());

			terminationFutures.add(ExecutorUtils.nonBlockingShutdown(timeout.toMilliseconds(), TimeUnit.MILLISECONDS, executor));

			if (exception != null) {
				terminationFutures.add(FutureUtils.completedExceptionally(exception));
			}

			return FutureUtils.completeAll(terminationFutures);
		}
	}

	// export the termination future for caller to know it is terminated
	public CompletableFuture<Void> getTerminationFuture() {
		return terminationFuture;
	}

	// --------------------------------------------------------------------------------------------
	//  FatalErrorHandler methods
	// --------------------------------------------------------------------------------------------

	@Override
	public void onFatalError(Throwable exception) {
		LOG.error("Fatal error occurred while executing the TaskManager. Shutting it down...", exception);

		if (ExceptionUtils.isJvmFatalOrOutOfMemoryError(exception)) {
			terminateJVM();
		} else {
			closeAsync();

			FutureUtils.orTimeout(terminationFuture, FATAL_ERROR_SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);

			terminationFuture.whenComplete(
				(Void ignored, Throwable throwable) -> {
					terminateJVM();
				});
		}
	}

	private void terminateJVM() {
		System.exit(RUNTIME_FAILURE_RETURN_CODE);
	}

	// --------------------------------------------------------------------------------------------
	//  Static entry point
	// --------------------------------------------------------------------------------------------

	public static void main(String[] args) throws Exception {
		// startup checks and logging //日志检测
		EnvironmentInformation.logEnvironmentInfo(LOG, "TaskManager", args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		long maxOpenFileHandles = EnvironmentInformation.getOpenFileHandlesLimit();

		if (maxOpenFileHandles != -1L) {
			LOG.info("Maximum number of open file descriptors is {}.", maxOpenFileHandles);
		} else {
			LOG.info("Cannot determine the maximum number of open file descriptors");
		}

		final Configuration configuration = loadConfiguration(args);

		try {
			FileSystem.initialize(configuration); //同样初始化file system
		} catch (IOException e) {
			throw new IOException("Error while setting the default " +
				"filesystem scheme from configuration.", e);
		}

		SecurityUtils.install(new SecurityConfiguration(configuration)); //初始化安全配置

		try {
			SecurityUtils.getInstalledContext().runSecured(new Callable<Void>() {
				@Override
				public Void call() throws Exception {
					runTaskManager(configuration, ResourceID.generate()); //这里
					return null;
				}
			});
		} catch (Throwable t) {
			final Throwable strippedThrowable = ExceptionUtils.stripException(t, UndeclaredThrowableException.class);
			LOG.error("TaskManager initialization failed.", strippedThrowable);
			System.exit(STARTUP_FAILURE_RETURN_CODE);
		}
	}

	@VisibleForTesting
	static Configuration loadConfiguration(String[] args) throws FlinkParseException {
		final CommandLineParser<ClusterConfiguration> commandLineParser = new CommandLineParser<>(new ClusterConfigurationParserFactory());

		final ClusterConfiguration clusterConfiguration;

		try {
			clusterConfiguration = commandLineParser.parse(args);
		} catch (FlinkParseException e) {
			LOG.error("Could not parse the command line options.", e);
			commandLineParser.printHelp(TaskManagerRunner.class.getSimpleName());
			throw e;
		}

		final Configuration dynamicProperties = ConfigurationUtils.createConfiguration(clusterConfiguration.getDynamicProperties());
		return GlobalConfiguration.loadConfiguration(clusterConfiguration.getConfigDir(), dynamicProperties);
	}

	public static void runTaskManager(Configuration configuration, ResourceID resourceId) throws Exception {
		final TaskManagerRunner taskManagerRunner = new TaskManagerRunner(configuration, resourceId);

		taskManagerRunner.start(); //从这里启动taskManager，启动了taskmanager的RPC服务，等待jobmanager分配job
	}

	// --------------------------------------------------------------------------------------------
	//  Static utilities
	// --------------------------------------------------------------------------------------------

	public static TaskExecutor startTaskManager(
			Configuration configuration,
			ResourceID resourceID,
			RpcService rpcService,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			MetricRegistry metricRegistry,
			BlobCacheService blobCacheService,
			boolean localCommunicationOnly,
			FatalErrorHandler fatalErrorHandler) throws Exception {

		checkNotNull(configuration);
		checkNotNull(resourceID);
		checkNotNull(rpcService);
		checkNotNull(highAvailabilityServices);

		LOG.info("Starting TaskManager with ResourceID: {}", resourceID);

		InetAddress remoteAddress = InetAddress.getByName(rpcService.getAddress()); //通过rpc的配置获得 连接的“句柄”

		TaskManagerServicesConfiguration taskManagerServicesConfiguration = //校验conf配置
			TaskManagerServicesConfiguration.fromConfiguration(
				configuration,
				remoteAddress,
				localCommunicationOnly);

		TaskManagerServices taskManagerServices = TaskManagerServices.fromConfiguration( //从配置中生成TaskManagerServices（该service包含很多功能，包括网络、内存、slot、state dir、异步I/O、超时service、job leader service）
			taskManagerServicesConfiguration,
			resourceID,
			rpcService.getExecutor(), // TODO replace this later with some dedicated executor for io.
			EnvironmentInformation.getSizeOfFreeHeapMemoryWithDefrag(),
			EnvironmentInformation.getMaxJvmHeapMemory());

		TaskManagerMetricGroup taskManagerMetricGroup = MetricUtils.instantiateTaskManagerMetricGroup( //metric
			metricRegistry,
			taskManagerServices.getTaskManagerLocation(),
			taskManagerServices.getNetworkEnvironment(),
			taskManagerServicesConfiguration.getSystemResourceMetricsProbingInterval());

		TaskManagerConfiguration taskManagerConfiguration = TaskManagerConfiguration.fromConfiguration(configuration); //获取taskmanager相关的conf

		String metricQueryServicePath = metricRegistry.getMetricQueryServicePath();

		return new TaskExecutor(//enter 通过上面配置的rpc配置、相关service、taskManager配置、Metrics Group得到TaskExecutor对象
			rpcService,
			taskManagerConfiguration,
			highAvailabilityServices,
			taskManagerServices,
			heartbeatServices,
			taskManagerMetricGroup,
			metricQueryServicePath,
			blobCacheService,
			fatalErrorHandler);
	}

	/**
	 * Create a RPC service for the task manager. //创建一个rpc服务 for task manager
	 *
	 * @param configuration The configuration for the TaskManager.
	 * @param haServices to use for the task manager hostname retrieval
	 */
	public static RpcService createRpcService(
			final Configuration configuration,
			final HighAvailabilityServices haServices) throws Exception {

		checkNotNull(configuration);
		checkNotNull(haServices);

		final String taskManagerAddress = determineTaskManagerBindAddress(configuration, haServices);
		final String portRangeDefinition = configuration.getString(TaskManagerOptions.RPC_PORT);

		return AkkaRpcServiceUtils.createRpcService(taskManagerAddress, portRangeDefinition, configuration); //通过taskmanager的 address/port 创建rpcService
	}

	private static String determineTaskManagerBindAddress(
			final Configuration configuration,
			final HighAvailabilityServices haServices) throws Exception {

		final String configuredTaskManagerHostname = configuration.getString(TaskManagerOptions.HOST);

		if (configuredTaskManagerHostname != null) {
			LOG.info("Using configured hostname/address for TaskManager: {}.", configuredTaskManagerHostname);
			return configuredTaskManagerHostname;
		} else {
			return determineTaskManagerBindAddressByConnectingToResourceManager(configuration, haServices);
		}
	}

	private static String determineTaskManagerBindAddressByConnectingToResourceManager(
			final Configuration configuration,
			final HighAvailabilityServices haServices) throws LeaderRetrievalException {

		final Time lookupTimeout = Time.milliseconds(AkkaUtils.getLookupTimeout(configuration).toMillis());

		final InetAddress taskManagerAddress = LeaderRetrievalUtils.findConnectingAddress(
			haServices.getResourceManagerLeaderRetriever(),
			lookupTimeout);

		LOG.info("TaskManager will use hostname/address '{}' ({}) for communication.",
			taskManagerAddress.getHostName(), taskManagerAddress.getHostAddress());

		HostBindPolicy bindPolicy = HostBindPolicy.fromString(configuration.getString(TaskManagerOptions.HOST_BIND_POLICY));
		return bindPolicy == HostBindPolicy.IP ? taskManagerAddress.getHostAddress() : taskManagerAddress.getHostName();
	}
}
