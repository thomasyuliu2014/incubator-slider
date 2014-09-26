/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.slider.server.appmaster;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.BlockingService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.ServiceStateChangeListener;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenSecretManager;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.slider.api.ClusterDescription;
import org.apache.slider.api.InternalKeys;
import org.apache.slider.api.ResourceKeys;
import org.apache.slider.api.RoleKeys;
import org.apache.slider.api.SliderClusterProtocol;
import org.apache.slider.api.StatusKeys;
import org.apache.slider.api.proto.Messages;
import org.apache.slider.api.proto.SliderClusterAPI;
import org.apache.slider.common.SliderExitCodes;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.params.AbstractActionArgs;
import org.apache.slider.common.params.SliderAMArgs;
import org.apache.slider.common.params.SliderAMCreateAction;
import org.apache.slider.common.params.SliderActions;
import org.apache.slider.common.tools.ConfigHelper;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.common.tools.SliderVersionInfo;
import org.apache.slider.core.build.InstanceIO;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.ConfTree;
import org.apache.slider.core.conf.ConfTreeOperations;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.exceptions.SliderInternalStateException;
import org.apache.slider.core.exceptions.TriggerClusterTeardownException;
import org.apache.slider.core.main.ExitCodeProvider;
import org.apache.slider.core.main.RunService;
import org.apache.slider.core.main.ServiceLauncher;
import org.apache.slider.core.persist.ConfTreeSerDeser;
import org.apache.slider.core.registry.info.CustomRegistryConstants;
import org.apache.slider.core.registry.info.RegisteredEndpoint;
import org.apache.slider.core.registry.info.RegistryNaming;
import org.apache.slider.core.registry.info.ServiceInstanceData;
import org.apache.slider.providers.ProviderCompleted;
import org.apache.slider.providers.ProviderRole;
import org.apache.slider.providers.ProviderService;
import org.apache.slider.providers.SliderProviderFactory;
import org.apache.slider.providers.slideram.SliderAMClientProvider;
import org.apache.slider.providers.slideram.SliderAMProviderService;
import org.apache.slider.server.appmaster.actions.ActionKillContainer;
import org.apache.slider.server.appmaster.actions.RegisterComponentInstance;
import org.apache.slider.server.appmaster.actions.QueueExecutor;
import org.apache.slider.server.appmaster.actions.ActionHalt;
import org.apache.slider.server.appmaster.actions.QueueService;
import org.apache.slider.server.appmaster.actions.ActionStopSlider;
import org.apache.slider.server.appmaster.actions.AsyncAction;
import org.apache.slider.server.appmaster.actions.RenewingAction;
import org.apache.slider.server.appmaster.actions.ResetFailureWindow;
import org.apache.slider.server.appmaster.actions.ReviewAndFlexApplicationSize;
import org.apache.slider.server.appmaster.actions.UnregisterComponentInstance;
import org.apache.slider.server.appmaster.monkey.ChaosKillAM;
import org.apache.slider.server.appmaster.monkey.ChaosKillContainer;
import org.apache.slider.server.appmaster.monkey.ChaosMonkeyService;
import org.apache.slider.server.appmaster.operations.AsyncRMOperationHandler;
import org.apache.slider.server.appmaster.operations.ProviderNotifyingOperationHandler;
import org.apache.slider.server.appmaster.rpc.RpcBinder;
import org.apache.slider.server.appmaster.rpc.SliderAMPolicyProvider;
import org.apache.slider.server.appmaster.rpc.SliderClusterProtocolPBImpl;
import org.apache.slider.server.appmaster.operations.AbstractRMOperation;
import org.apache.slider.server.appmaster.state.AppState;
import org.apache.slider.server.appmaster.state.ContainerAssignment;
import org.apache.slider.server.appmaster.state.ProviderAppState;
import org.apache.slider.server.appmaster.operations.RMOperationHandler;
import org.apache.slider.server.appmaster.state.RoleInstance;
import org.apache.slider.server.appmaster.state.RoleStatus;
import org.apache.slider.server.appmaster.state.SimpleReleaseSelector;
import org.apache.slider.server.appmaster.web.AgentService;
import org.apache.slider.server.appmaster.web.rest.agent.AgentWebApp;
import org.apache.slider.server.appmaster.web.SliderAMWebApp;
import org.apache.slider.server.appmaster.web.WebAppApi;
import org.apache.slider.server.appmaster.web.WebAppApiImpl;
import org.apache.slider.server.appmaster.web.rest.RestPaths;
import org.apache.slider.server.services.registry.SliderRegistryService;
import org.apache.slider.server.services.security.CertificateManager;
import org.apache.slider.server.services.security.FsDelegationTokenManager;
import org.apache.slider.server.services.utility.AbstractSliderLaunchedService;
import org.apache.slider.server.services.utility.WebAppService;
import org.apache.slider.server.services.workflow.ServiceThreadFactory;
import org.apache.slider.server.services.workflow.WorkflowExecutorService;
import org.apache.slider.server.services.workflow.WorkflowRpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This is the AM, which directly implements the callbacks from the AM and NM
 */
public class SliderAppMaster extends AbstractSliderLaunchedService 
  implements AMRMClientAsync.CallbackHandler,
    NMClientAsync.CallbackHandler,
    RunService,
    SliderExitCodes,
    SliderKeys,
    SliderClusterProtocol,
    ServiceStateChangeListener,
    RoleKeys,
    ProviderCompleted {
  protected static final Logger log =
    LoggerFactory.getLogger(SliderAppMaster.class);

  /**
   * log for YARN events
   */
  protected static final Logger LOG_YARN = log;

  public static final String SERVICE_CLASSNAME_SHORT =
      "SliderAppMaster";
  public static final String SERVICE_CLASSNAME =
      "org.apache.slider.server.appmaster." + SERVICE_CLASSNAME_SHORT;


  /**
   * time to wait from shutdown signal being rx'd to telling
   * the AM: {@value}
   */
  public static final int TERMINATION_SIGNAL_PROPAGATION_DELAY = 1000;

  public static final int HEARTBEAT_INTERVAL = 1000;
  public static final int NUM_RPC_HANDLERS = 5;
  public static final int SCHEDULED_EXECUTOR_POOL_SIZE = 1;

  /**
   * Singleton of metrics registry
   */
  public static final MetricRegistry metrics = new MetricRegistry();
  
  /** YARN RPC to communicate with the Resource Manager or Node Manager */
  private YarnRPC yarnRPC;

  /** Handle to communicate with the Resource Manager*/
  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
  private AMRMClientAsync asyncRMClient;

  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")

  private RMOperationHandler rmOperationHandler;
  
  private RMOperationHandler providerRMOperationHandler;

  /** Handle to communicate with the Node Manager*/
  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
  public NMClientAsync nmClientAsync;

//  YarnConfiguration conf;
  /**
   * token blob
   */
  private ByteBuffer allTokens;

  private WorkflowRpcService rpcService;

  /**
   * Secret manager
   */
  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
  private ClientToAMTokenSecretManager secretManager;
  
  /** Hostname of the container*/
  private String appMasterHostname = "";
  /* Port on which the app master listens for status updates from clients*/
  private int appMasterRpcPort = 0;
  /** Tracking url to which app master publishes info for clients to monitor*/
  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
  private String appMasterTrackingUrl = "";

  /** Application Attempt Id ( combination of attemptId and fail count )*/
  private ApplicationAttemptId appAttemptID;

  /**
   * Security info client to AM key returned after registration
   */
  private ByteBuffer clientToAMKey;

  /**
   * App ACLs
   */
  protected Map<ApplicationAccessType, String> applicationACLs;

  /**
   * Ongoing state of the cluster: containers, nodes they
   * live on, etc.
   */
  private final AppState appState = new AppState(new ProtobufRecordFactory());

  private final ProviderAppState stateForProviders =
      new ProviderAppState("undefined", appState);


  /**
   * model the state using locks and conditions
   */
  private final ReentrantLock AMExecutionStateLock = new ReentrantLock();
  private final Condition isAMCompleted = AMExecutionStateLock.newCondition();

  /**
   * Exit code for the AM to return
   */
  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
  private int amExitCode =  0;
  
  /**
   * Flag set if the AM is to be shutdown
   */
  private final AtomicBoolean amCompletionFlag = new AtomicBoolean(false);

  /**
   * Flag set during the init process
   */
  private final AtomicBoolean initCompleted = new AtomicBoolean(false);
  
  private volatile boolean success = true;

  /**
   * Flag to set if the process exit code was set before shutdown started
   */
  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
  private boolean spawnedProcessExitedBeforeShutdownTriggered;


  /** Arguments passed in : raw*/
  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
  private SliderAMArgs serviceArgs;

  /**
   * ID of the AM container
   */
  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
  private ContainerId appMasterContainerID;

  /**
   * Monkey Service -may be null
   */
  private ChaosMonkeyService monkey;
  
  /**
   * ProviderService of this cluster
   */
  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
  private ProviderService providerService;

  /**
   * The registry service
   */
  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
  private SliderRegistryService registry;
  
  /**
   * Record of the max no. of cores allowed in this cluster
   */
  private int containerMaxCores;

  /**
   * limit container memory
   */
  private int containerMaxMemory;
  private String amCompletionReason;

  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
  private RoleLaunchService launchService;
  
  //username -null if it is not known/not to be set
  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
  private String hadoop_user_name;
  private String service_user_name;
  
  private SliderAMWebApp webApp;
  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
  private InetSocketAddress rpcServiceAddress;
  private ProviderService sliderAMProvider;
  private CertificateManager certificateManager;

  private WorkflowExecutorService<ExecutorService> executorService;
  
  private final QueueService actionQueues = new QueueService();
  private String agentOpsUrl;
  private String agentStatusUrl;
  private FsDelegationTokenManager fsDelegationTokenManager;

  /**
   * Service Constructor
   */
  public SliderAppMaster() {
    super(SERVICE_CLASSNAME_SHORT);
  }

/* =================================================================== */
/* service lifecycle methods */
/* =================================================================== */

  @Override //AbstractService
  public synchronized void serviceInit(Configuration conf) throws Exception {

    // Load in the server configuration - if it is actually on the Classpath
    Configuration serverConf =
      ConfigHelper.loadFromResource(SERVER_RESOURCE);
    ConfigHelper.mergeConfigurations(conf, serverConf, SERVER_RESOURCE);

    AbstractActionArgs action = serviceArgs.getCoreAction();
    SliderAMCreateAction createAction = (SliderAMCreateAction) action;
    //sort out the location of the AM
    serviceArgs.applyDefinitions(conf);
    serviceArgs.applyFileSystemBinding(conf);

    String rmAddress = createAction.getRmAddress();
    if (rmAddress != null) {
      log.debug("Setting rm address from the command line: {}", rmAddress);
      SliderUtils.setRmSchedulerAddress(conf, rmAddress);
    }
    serviceArgs.applyDefinitions(conf);
    serviceArgs.applyFileSystemBinding(conf);
    //init security with our conf
    if (SliderUtils.isHadoopClusterSecure(conf)) {
      log.info("Secure mode with kerberos realm {}",
               SliderUtils.getKerberosRealm());
      UserGroupInformation.setConfiguration(conf);
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      log.debug("Authenticating as {}", ugi);
      SliderUtils.verifyPrincipalSet(conf,
          DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY);
      // always enforce protocol to be token-based.
      conf.set(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        SaslRpcServer.AuthMethod.TOKEN.toString());
    }
    log.info("Login user is {}", UserGroupInformation.getLoginUser());

    //look at settings of Hadoop Auth, to pick up a problem seen once
    checkAndWarnForAuthTokenProblems();
    
    // validate server env
    SliderUtils.validateSliderServerEnvironment(log);

    executorService = new WorkflowExecutorService<ExecutorService>("AmExecutor",
        Executors.newCachedThreadPool(
        new ServiceThreadFactory("AmExecutor", true)));
    addService(executorService);


    addService(actionQueues);
    //init all child services
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
  }

  /**
   * Start the queue processing
   */
  private void startQueueProcessing() {
    log.info("Queue Processing started");
    executorService.execute(actionQueues);
    executorService.execute(new QueueExecutor(this, actionQueues));
  }
  
/* =================================================================== */
/* RunService methods called from ServiceLauncher */
/* =================================================================== */

  /**
   * pick up the args from the service launcher
   * @param config configuration
   * @param args argument list
   */
  @Override // RunService
  public Configuration bindArgs(Configuration config, String... args) throws
                                                                      Exception {
    YarnConfiguration yarnConfiguration = new YarnConfiguration(
        super.bindArgs(config, args));
    serviceArgs = new SliderAMArgs(args);
    serviceArgs.parse();
    //yarn-ify
    return SliderUtils.patchConfiguration(yarnConfiguration);
  }


  /**
   * this is called by service launcher; when it returns the application finishes
   * @return the exit code to return by the app
   * @throws Throwable
   */
  @Override
  public int runService() throws Throwable {
    SliderVersionInfo.loadAndPrintVersionInfo(log);

    //dump the system properties if in debug mode
    if (log.isDebugEnabled()) {
      log.debug("System properties:\n" +
                SliderUtils.propertiesToString(System.getProperties()));
    }

    //choose the action
    String action = serviceArgs.getAction();
    List<String> actionArgs = serviceArgs.getActionArgs();
    int exitCode;
/*  JDK7
  switch (action) {
      case SliderActions.ACTION_HELP:
        log.info(getName() + serviceArgs.usage());
        exitCode = LauncherExitCodes.EXIT_USAGE;
        break;
      case SliderActions.ACTION_CREATE:
        exitCode = createAndRunCluster(actionArgs.get(0));
        break;
      default:
        throw new SliderException("Unimplemented: " + action);
    }
    */
    if (action.equals(SliderActions.ACTION_HELP)) {
      log.info(getName() + serviceArgs.usage());
      exitCode = SliderExitCodes.EXIT_USAGE;
    } else if (action.equals(SliderActions.ACTION_CREATE)) {
      exitCode = createAndRunCluster(actionArgs.get(0));
    } else {
      throw new SliderException("Unimplemented: " + action);
    }
    log.info("Exiting AM; final exit code = {}", exitCode);
    return exitCode;
  }


  /**
   * Initialize a newly created service then add it. 
   * Because the service is not started, this MUST be done before
   * the AM itself starts, or it is explicitly added after
   * @param service the service to init
   */
  public Service initAndAddService(Service service) {
    service.init(getConfig());
    addService(service);
    return service;
  }

  /* =================================================================== */

  /**
   * Create and run the cluster.
   * @return exit code
   * @throws Throwable on a failure
   */
  private int createAndRunCluster(String clustername) throws Throwable {

    //load the cluster description from the cd argument
    String sliderClusterDir = serviceArgs.getSliderClusterURI();
    URI sliderClusterURI = new URI(sliderClusterDir);
    Path clusterDirPath = new Path(sliderClusterURI);
    SliderFileSystem fs = getClusterFS();

    // build up information about the running application -this
    // will be passed down to the cluster status
    MapOperations appInformation = new MapOperations(); 

    AggregateConf instanceDefinition =
      InstanceIO.loadInstanceDefinitionUnresolved(fs, clusterDirPath);
    instanceDefinition.setName(clustername);

    log.info("Deploying cluster {}:", instanceDefinition);

    stateForProviders.setApplicationName(clustername);
    
    // triggers resolution and snapshotting in agent
    appState.updateInstanceDefinition(instanceDefinition);
    File confDir = getLocalConfDir();
    if (!confDir.exists() || !confDir.isDirectory()) {
      log.info("Conf dir {} does not exist.", confDir);
      File parentFile = confDir.getParentFile();
      log.info("Parent dir {}:\n{}", parentFile, SliderUtils.listDir(parentFile));
    }

    Configuration serviceConf = getConfig();
    // IP filtering 
    serviceConf.set(HADOOP_HTTP_FILTER_INITIALIZERS, AM_FILTER_NAME);
    
    //get our provider
    MapOperations globalInternalOptions = getGlobalInternalOptions();
    String providerType = globalInternalOptions.getMandatoryOption(
      InternalKeys.INTERNAL_PROVIDER_NAME);
    log.info("Cluster provider type is {}", providerType);
    SliderProviderFactory factory =
      SliderProviderFactory.createSliderProviderFactory(
          providerType);
    providerService = factory.createServerProvider();
    // init the provider BUT DO NOT START IT YET
    initAndAddService(providerService);
    providerRMOperationHandler =
        new ProviderNotifyingOperationHandler(providerService);
    
    // create a slider AM provider
    sliderAMProvider = new SliderAMProviderService();
    initAndAddService(sliderAMProvider);
    
    InetSocketAddress address = SliderUtils.getRmSchedulerAddress(serviceConf);
    log.info("RM is at {}", address);
    yarnRPC = YarnRPC.create(serviceConf);

    /*
     * Extract the container ID. This is then
     * turned into an (incompete) container
     */
    appMasterContainerID = ConverterUtils.toContainerId(
      SliderUtils.mandatoryEnvVariable(
          ApplicationConstants.Environment.CONTAINER_ID.name())
                                                       );
    appAttemptID = appMasterContainerID.getApplicationAttemptId();

    ApplicationId appid = appAttemptID.getApplicationId();
    log.info("AM for ID {}", appid.getId());

    appInformation.put(StatusKeys.INFO_AM_CONTAINER_ID,
                       appMasterContainerID.toString());
    appInformation.put(StatusKeys.INFO_AM_APP_ID,
                       appid.toString());
    appInformation.put(StatusKeys.INFO_AM_ATTEMPT_ID,
                       appAttemptID.toString());

    Map<String, String> envVars;
    List<Container> liveContainers;
    /**
     * It is critical this section is synchronized, to stop async AM events
     * arriving while registering a restarting AM.
     */
    synchronized (appState) {
      int heartbeatInterval = HEARTBEAT_INTERVAL;

      //add the RM client -this brings the callbacks in
      asyncRMClient = AMRMClientAsync.createAMRMClientAsync(heartbeatInterval,
                                                            this);
      addService(asyncRMClient);
      //wrap it for the app state model
      rmOperationHandler = new AsyncRMOperationHandler(asyncRMClient);
      //now bring it up
      deployChildService(asyncRMClient);


      //nmclient relays callbacks back to this class
      nmClientAsync = new NMClientAsyncImpl("nmclient", this);
      deployChildService(nmClientAsync);

      // set up secret manager
      secretManager = new ClientToAMTokenSecretManager(appAttemptID, null);

      //bring up the Slider RPC service
      startSliderRPCServer();

      rpcServiceAddress = rpcService.getConnectAddress();
      appMasterHostname = rpcServiceAddress.getHostName();
      appMasterRpcPort = rpcServiceAddress.getPort();
      appMasterTrackingUrl = null;
      log.info("AM Server is listening at {}:{}", appMasterHostname,
               appMasterRpcPort);
      appInformation.put(StatusKeys.INFO_AM_HOSTNAME, appMasterHostname);
      appInformation.set(StatusKeys.INFO_AM_RPC_PORT, appMasterRpcPort);

      
      //registry
      registry = startRegistrationService();

      //build the role map
      List<ProviderRole> providerRoles =
        new ArrayList<ProviderRole>(providerService.getRoles());
      providerRoles.addAll(SliderAMClientProvider.ROLES);

      // Start up the WebApp and track the URL for it
      certificateManager = new CertificateManager();
      certificateManager.initRootCert(
          instanceDefinition.getAppConfOperations()
              .getComponent(SliderKeys.COMPONENT_AM));

      startAgentWebApp(appInformation, serviceConf);

      webApp = new SliderAMWebApp(registry);
      WebApps.$for(SliderAMWebApp.BASE_PATH, WebAppApi.class,
                   new WebAppApiImpl(this,
                                     stateForProviders,
                                     providerService,
                                     certificateManager),
                   RestPaths.WS_CONTEXT)
                      .with(serviceConf)
                      .start(webApp);
      String scheme = WebAppUtils.getHttpSchemePrefix(serviceConf);
      appMasterTrackingUrl = scheme  + appMasterHostname + ":" + webApp.port();
      WebAppService<SliderAMWebApp> webAppService =
        new WebAppService<SliderAMWebApp>("slider", webApp);

      webAppService.init(serviceConf);
      webAppService.start();
      addService(webAppService);

      appInformation.put(StatusKeys.INFO_AM_WEB_URL, appMasterTrackingUrl + "/");
      appInformation.set(StatusKeys.INFO_AM_WEB_PORT, webApp.port());

      // Register self with ResourceManager
      // This will start heartbeating to the RM
      // address = SliderUtils.getRmSchedulerAddress(asyncRMClient.getConfig());
      log.info("Connecting to RM at {},address tracking URL={}",
               appMasterRpcPort, appMasterTrackingUrl);
      RegisterApplicationMasterResponse response = asyncRMClient
        .registerApplicationMaster(appMasterHostname,
                                   appMasterRpcPort,
                                   appMasterTrackingUrl);
      Resource maxResources =
        response.getMaximumResourceCapability();
      containerMaxMemory = maxResources.getMemory();
      containerMaxCores = maxResources.getVirtualCores();
      appState.setContainerLimits(maxResources.getMemory(),
                                  maxResources.getVirtualCores());
      // set the RM-defined maximum cluster values
      appInformation.put(ResourceKeys.YARN_CORES, Integer.toString(containerMaxCores));
      appInformation.put(ResourceKeys.YARN_MEMORY, Integer.toString(containerMaxMemory));
      
      boolean securityEnabled = UserGroupInformation.isSecurityEnabled();
      if (securityEnabled) {
        secretManager.setMasterKey(
          response.getClientToAMTokenMasterKey().array());
        applicationACLs = response.getApplicationACLs();

        //tell the server what the ACLs are 
        rpcService.getServer().refreshServiceAcl(serviceConf,
            new SliderAMPolicyProvider());
      }

      // extract container list

      liveContainers = response.getContainersFromPreviousAttempts();

      //now validate the installation
      Configuration providerConf =
        providerService.loadProviderConfigurationInformation(confDir);

      providerService
          .initializeApplicationConfiguration(instanceDefinition, fs);

      providerService.validateApplicationConfiguration(instanceDefinition, 
                                                       confDir,
                                                       securityEnabled);

      //determine the location for the role history data
      Path historyDir = new Path(clusterDirPath, HISTORY_DIR_NAME);

      //build the instance
      appState.buildInstance(instanceDefinition,
          serviceConf,
          providerConf,
          providerRoles,
          fs.getFileSystem(),
          historyDir,
          liveContainers,
          appInformation,
          new SimpleReleaseSelector());

      providerService.rebuildContainerDetails(liveContainers,
          instanceDefinition.getName(), appState.getRolePriorityMap());

      // add the AM to the list of nodes in the cluster
      
      appState.buildAppMasterNode(appMasterContainerID,
                                  appMasterHostname,
                                  webApp.port(),
                                  appMasterHostname + ":" + webApp.port());

      // build up environment variables that the AM wants set in every container
      // irrespective of provider and role.
      envVars = new HashMap<String, String>();
      if (hadoop_user_name != null) {
        envVars.put(HADOOP_USER_NAME, hadoop_user_name);
      }
    }
    String rolesTmpSubdir = appMasterContainerID.toString() + "/roles";

    String amTmpDir = globalInternalOptions.getMandatoryOption(InternalKeys.INTERNAL_AM_TMP_DIR);

    Path tmpDirPath = new Path(amTmpDir);
    Path launcherTmpDirPath = new Path(tmpDirPath, rolesTmpSubdir);
    fs.getFileSystem().mkdirs(launcherTmpDirPath);
    
    //launcher service
    launchService = new RoleLaunchService(actionQueues,
                                          providerService,
                                          fs,
                                          new Path(getGeneratedConfDir()),
                                          envVars,
                                          launcherTmpDirPath);

    deployChildService(launchService);

    appState.noteAMLaunched();


    //Give the provider restricted access to the state, registry
    providerService.bind(stateForProviders, registry, actionQueues,
        liveContainers);
    sliderAMProvider.bind(stateForProviders, registry, actionQueues,
        liveContainers);

    // chaos monkey
    maybeStartMonkey();

    // setup token renewal and expiry handling for long lived apps
    if (SliderUtils.isHadoopClusterSecure(getConfig())) {
      fsDelegationTokenManager = new FsDelegationTokenManager(actionQueues);
      fsDelegationTokenManager.acquireDelegationToken(getConfig());
    }

    UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
    Credentials credentials =
        currentUser.getCredentials();
    DataOutputBuffer dob = new DataOutputBuffer();
    credentials.writeTokenStorageToStream(dob);
    dob.close();
    // Now remove the AM->RM token so that containers cannot access it.
    Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
    while (iter.hasNext()) {
      Token<?> token = iter.next();
      log.info("Token {}", token.getKind());
      if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
        iter.remove();
      }
    }
    allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

    // if not a secure cluster, extract the username -it will be
    // propagated to workers
    if (!UserGroupInformation.isSecurityEnabled()) {
      hadoop_user_name = System.getenv(HADOOP_USER_NAME);
      service_user_name = hadoop_user_name;
      log.info(HADOOP_USER_NAME + "='{}'", hadoop_user_name);
    } else {
      service_user_name = UserGroupInformation.getCurrentUser().getUserName();
    }

    // now do the registration
    registerServiceInstance(clustername, appid);

    // declare the cluster initialized
    log.info("Application Master Initialization Completed");
    initCompleted.set(true);

    // start handling any scheduled events
    
    startQueueProcessing();
    // Start the Slider AM provider
    
    sliderAMProvider.start();

    // launch the real provider; this is expected to trigger a callback that
    // starts the node review process
    launchProviderService(instanceDefinition, confDir);

    try {
      //now block waiting to be told to exit the process
      waitForAMCompletionSignal();
      //shutdown time
    } finally {
      finish();
    }

    return amExitCode;
  }

  private void startAgentWebApp(MapOperations appInformation,
                                Configuration serviceConf) throws IOException {
    URL[] urls = ((URLClassLoader) AgentWebApp.class.getClassLoader() ).getURLs();
    StringBuilder sb = new StringBuilder("AM classpath:");
    for (URL url : urls) {
      sb.append("\n").append(url.toString());
    }
    LOG_YARN.info(sb.append("\n").toString());
    // Start up the agent web app and track the URL for it
    AgentWebApp agentWebApp = AgentWebApp.$for(AgentWebApp.BASE_PATH,
                     new WebAppApiImpl(this,
                                       stateForProviders,
                                       providerService,
                                       certificateManager),
                     RestPaths.AGENT_WS_CONTEXT)
        .withComponentConfig(getInstanceDefinition().getAppConfOperations()
                                 .getComponent(SliderKeys.COMPONENT_AM))
        .start();
    agentOpsUrl =
        "https://" + appMasterHostname + ":" + agentWebApp.getSecuredPort();
    agentStatusUrl =
        "https://" + appMasterHostname + ":" + agentWebApp.getPort();
    AgentService agentService =
      new AgentService("slider-agent", agentWebApp);

    agentService.init(serviceConf);
    agentService.start();
    addService(agentService);

    appInformation.put(StatusKeys.INFO_AM_AGENT_OPS_URL, agentOpsUrl + "/");
    appInformation.put(StatusKeys.INFO_AM_AGENT_STATUS_URL, agentStatusUrl + "/");
    appInformation.set(StatusKeys.INFO_AM_AGENT_STATUS_PORT, agentWebApp.getPort());
    appInformation.set(StatusKeys.INFO_AM_AGENT_OPS_PORT,
                       agentWebApp.getSecuredPort());
  }

  /**
   * This registers the service instance and its external values
   * @param instanceName name of this instance
   * @param appid application ID
   * @throws Exception
   */
  private void registerServiceInstance(String instanceName,
      ApplicationId appid) throws Exception {
    // the registry is running, so register services
    URL amWebURI = new URL(appMasterTrackingUrl);
    URL agentOpsURI = new URL(agentOpsUrl);
    URL agentStatusURI = new URL(agentStatusUrl);
    String serviceName = SliderKeys.APP_TYPE;
    int id = appid.getId();
    String appServiceType = RegistryNaming.createRegistryServiceType(
        instanceName,
        service_user_name,
        serviceName);
    String registryId =
      RegistryNaming.createRegistryName(instanceName, service_user_name,
          serviceName, id);

    List<String> serviceInstancesRunning = registry.instanceIDs(serviceName);
    log.info("service instances already running: {}", serviceInstancesRunning);


    ServiceInstanceData instanceData = new ServiceInstanceData(registryId,
        appServiceType);


    // IPC services
    instanceData.externalView.endpoints.put(
        CustomRegistryConstants.AM_IPC_PROTOCOL,
        new RegisteredEndpoint(rpcServiceAddress,
            RegisteredEndpoint.PROTOCOL_HADOOP_PROTOBUF,
            "Slider AM RPC") );


    // internal services
   
    sliderAMProvider.applyInitialRegistryDefinitions(amWebURI,
                                                     agentOpsURI,
                                                     agentStatusURI,
                                                     instanceData);

    // provider service dynamic definitions.
    providerService.applyInitialRegistryDefinitions(amWebURI,
                                                    agentOpsURI,
                                                    agentStatusURI,
                                                    instanceData);


    // push the registration info to ZK

    registry.registerSelf(
        instanceData, amWebURI
    );
  }

  /**
   * Register/re-register a component (that is already in the app state
   * @param id the component
   */
  public boolean registerComponent(ContainerId id) {
    RoleInstance instance = appState.getOwnedContainer(id);
    if (instance == null) {
      return false;
    }
    // this is where component registrations will go
    log.info("Registering component {}", id);

    return true;
  }
  
  /**
   * unregister a component. At the time this message is received,
   * the component may already been deleted from/never added to
   * the app state
   * @param id the component
   */
  public void unregisterComponent(ContainerId id) {
    log.info("Unregistering component {}", id);
  }
  
  /**
   * looks for a specific case where a token file is provided as an environment
   * variable, yet the file is not there.
   * 
   * This surfaced (once) in HBase, where its HDFS library was looking for this,
   * and somehow the token was missing. This is a check in the AM so that
   * if the problem re-occurs, the AM can fail with a more meaningful message.
   * 
   */
  private void checkAndWarnForAuthTokenProblems() {
    String fileLocation =
      System.getenv(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);
    if (fileLocation != null) {
      File tokenFile = new File(fileLocation);
      if (!tokenFile.exists()) {
        log.warn("Token file {} specified in {} not found", tokenFile,
                 UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);
      }
    }
  }

  /**
   * Build the configuration directory passed in or of the target FS
   * @return the file
   */
  public File getLocalConfDir() {
    File confdir =
      new File(SliderKeys.PROPAGATED_CONF_DIR_NAME).getAbsoluteFile();
    return confdir;
  }

  /**
   * Get the path to the DFS configuration that is defined in the cluster specification 
   * @return the generated configuration dir
   */
  public String getGeneratedConfDir() {
    return getGlobalInternalOptions().get(
        InternalKeys.INTERNAL_GENERATED_CONF_PATH);
  }

  /**
   * Get the global internal options for the AM
   * @return a map to access the internals
   */
  public MapOperations getGlobalInternalOptions() {
    return getInstanceDefinition()
      .getInternalOperations().
      getGlobalOptions();
  }

  /**
   * Get the filesystem of this cluster
   * @return the FS of the config
   */
  public SliderFileSystem getClusterFS() throws IOException {
    return new SliderFileSystem(getConfig());
  }

  /**
   * Get the AM log
   * @return the log of the AM
   */
  public static Logger getLog() {
    return log;
  }

  /**
   * Get the application state
   * @return the application state
   */
  public AppState getAppState() {
    return appState;
  }

  /**
   * Block until it is signalled that the AM is done
   */
  private void waitForAMCompletionSignal() {
    AMExecutionStateLock.lock();
    try {
      if (!amCompletionFlag.get()) {
        log.debug("blocking until signalled to terminate");
        isAMCompleted.awaitUninterruptibly();
      }
    } finally {
      AMExecutionStateLock.unlock();
    }
    //add a sleep here for about a second. Why? it
    //stops RPC calls breaking so dramatically when the cluster
    //is torn down mid-RPC
    try {
      Thread.sleep(TERMINATION_SIGNAL_PROPAGATION_DELAY);
    } catch (InterruptedException ignored) {
      //ignored
    }
  }

  /**
   * Declare that the AM is complete
   * @param exitCode exit code for the aM
   * @param reason reason for termination
   */
  public synchronized void signalAMComplete(int exitCode, String reason) {
    amCompletionReason = reason;
    AMExecutionStateLock.lock();
    try {
      amCompletionFlag.set(true);
      amExitCode = exitCode;
      isAMCompleted.signal();
    } finally {
      AMExecutionStateLock.unlock();
    }
  }

  /**
   * shut down the cluster 
   */
  private synchronized void finish() {
    FinalApplicationStatus appStatus;
    log.info("Triggering shutdown of the AM: {}", amCompletionReason);

    String appMessage = amCompletionReason;
    //stop the daemon & grab its exit code
    int exitCode = amExitCode;
    success = exitCode == 0 || exitCode == 3;

    appStatus = success ? FinalApplicationStatus.SUCCEEDED:
                FinalApplicationStatus.FAILED;
    if (!spawnedProcessExitedBeforeShutdownTriggered) {
      //stopped the forked process but don't worry about its exit code
      exitCode = stopForkedProcess();
      log.debug("Stopped forked process: exit code={}", exitCode);
    }

    if (fsDelegationTokenManager != null) {
      try {
        fsDelegationTokenManager.cancelDelegationToken(getConfig());
      } catch (Exception e) {
        log.info("Error cancelling HDFS delegation token", e);
      }
    }

    //stop any launches in progress
    launchService.stop();

    //now release all containers
    releaseAllContainers();

    // When the application completes, it should send a finish application
    // signal to the RM
    log.info("Application completed. Signalling finish to RM");

    //if there were failed containers and the app isn't already down as failing, it is now
/*
    int failedContainerCount = appState.getFailedCountainerCount();
    if (failedContainerCount != 0 &&
        appStatus == FinalApplicationStatus.SUCCEEDED) {
      appStatus = FinalApplicationStatus.FAILED;
      appMessage =
        "Completed with exit code =  " + exitCode + " - " + getContainerDiagnosticInfo();
      success = false;
    }
*/
    try {
      log.info("Unregistering AM status={} message={}", appStatus, appMessage);
      asyncRMClient.unregisterApplicationMaster(appStatus, appMessage, null);
/* JDK7
    } catch (YarnException | IOException e) {
      log.info("Failed to unregister application: " + e, e);
    }
*/
    } catch (IOException e) {
      log.info("Failed to unregister application: " + e, e);
    } catch (YarnException e) {
      log.info("Failed to unregister application: " + e, e);
    }
  }

  /**
   * Get diagnostics info about containers
   */
  private String getContainerDiagnosticInfo() {
   return appState.getContainerDiagnosticInfo();
  }

  public Object getProxy(Class protocol, InetSocketAddress addr) {
    return yarnRPC.getProxy(protocol, addr, getConfig());
  }

  /**
   * Start the slider RPC server
   */
  private void startSliderRPCServer() throws IOException {
    SliderClusterProtocolPBImpl protobufRelay = new SliderClusterProtocolPBImpl(this);
    BlockingService blockingService = SliderClusterAPI.SliderClusterProtocolPB
                                                    .newReflectiveBlockingService(
                                                      protobufRelay);

    rpcService = new WorkflowRpcService("SliderRPC", RpcBinder.createProtobufServer(
      new InetSocketAddress("0.0.0.0", 0),
      getConfig(),
      secretManager,
      NUM_RPC_HANDLERS,
      blockingService,
      null));
    deployChildService(rpcService);
  }


/* =================================================================== */
/* AMRMClientAsync callbacks */
/* =================================================================== */

  /**
   * Callback event when a container is allocated.
   * 
   * The app state is updated with the allocation, and builds up a list
   * of assignments and RM opreations. The assignments are 
   * handed off into the pool of service launchers to asynchronously schedule
   * container launch operations.
   * 
   * The operations are run in sequence; they are expected to be 0 or more
   * release operations (to handle over-allocations)
   * 
   * @param allocatedContainers list of containers that are now ready to be
   * given work.
   */
  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  @Override //AMRMClientAsync
  public void onContainersAllocated(List<Container> allocatedContainers) {
    LOG_YARN.info("onContainersAllocated({})", allocatedContainers.size());
    List<ContainerAssignment> assignments = new ArrayList<ContainerAssignment>();
    List<AbstractRMOperation> operations = new ArrayList<AbstractRMOperation>();
    
    //app state makes all the decisions
    appState.onContainersAllocated(allocatedContainers, assignments, operations);

    //for each assignment: instantiate that role
    for (ContainerAssignment assignment : assignments) {
      RoleStatus role = assignment.role;
      Container container = assignment.container;
      launchService.launchRole(container, role, getInstanceDefinition());
    }
    
    //for all the operations, exec them
    executeRMOperations(operations);
    log.info("Diagnostics: " + getContainerDiagnosticInfo());
  }

  @Override //AMRMClientAsync
  public synchronized void onContainersCompleted(List<ContainerStatus> completedContainers) {
    LOG_YARN.info("onContainersCompleted([{}]", completedContainers.size());
    for (ContainerStatus status : completedContainers) {
      ContainerId containerId = status.getContainerId();
      LOG_YARN.info("Container Completion for" +
                    " containerID={}," +
                    " state={}," +
                    " exitStatus={}," +
                    " diagnostics={}",
                    containerId, status.getState(),
                    status.getExitStatus(),
                    status.getDiagnostics());

      // non complete containers should not be here
      assert (status.getState() == ContainerState.COMPLETE);
      AppState.NodeCompletionResult result = appState.onCompletedNode(status);
      if (result.containerFailed) {
        RoleInstance ri = result.roleInstance;
        log.error("Role instance {} failed ", ri);
      }

      //  known nodes trigger notifications
      if(!result.unknownNode) {
        getProviderService().notifyContainerCompleted(containerId);
        queue(new UnregisterComponentInstance(containerId, 0, TimeUnit.MILLISECONDS));
      }
    }

    reviewRequestAndReleaseNodes("onContainersCompleted");
  }

  /**
   * Implementation of cluster flexing.
   * It should be the only way that anything -even the AM itself on startup-
   * asks for nodes. 
   * @param resources the resource tree
   * @throws IOException
   */
  private void flexCluster(ConfTree resources)
    throws IOException, SliderInternalStateException, BadConfigException {

    appState.updateResourceDefinitions(resources);

    // reset the scheduled windows...the values
    // may have changed
    appState.resetFailureCounts();
    


    // ask for more containers if needed
    reviewRequestAndReleaseNodes("flexCluster");
  }

  /**
   * Schedule the failure window
   * @param resources the resource tree
   * @throws BadConfigException if the window is out of range
   */
  private void scheduleFailureWindowResets(ConfTree resources) throws
      BadConfigException {
    ResetFailureWindow reset = new ResetFailureWindow();
    ConfTreeOperations ops = new ConfTreeOperations(resources);
    MapOperations globals = ops.getGlobalOptions();
    long seconds = globals.getTimeRange(ResourceKeys.CONTAINER_FAILURE_WINDOW,
        ResourceKeys.DEFAULT_CONTAINER_FAILURE_WINDOW_DAYS,
        ResourceKeys.DEFAULT_CONTAINER_FAILURE_WINDOW_HOURS,
        ResourceKeys.DEFAULT_CONTAINER_FAILURE_WINDOW_MINUTES, 0);
    if (seconds > 0) {
      log.info(
          "Scheduling the failure window reset interval to every {} seconds",
          seconds);
      RenewingAction<ResetFailureWindow> renew = new RenewingAction<ResetFailureWindow>(
          reset, seconds, seconds, TimeUnit.SECONDS, 0);
      actionQueues.renewing("failures", renew);
    } else {
      log.info("Failure window reset interval is not set");
    }
  }
  
  /**
   * Look at where the current node state is -and whether it should be changed
   * @param reason
   */
  private synchronized void reviewRequestAndReleaseNodes(String reason) {
    log.debug("reviewRequestAndReleaseNodes({})", reason);
    queue(new ReviewAndFlexApplicationSize(reason, 0, TimeUnit.SECONDS));
  }

  /**
   * Handle the event requesting a review ... look at the queue and decide
   * whether to act or not
   * @param action action triggering the event. It may be put
   * back into the queue
   * @throws SliderInternalStateException
   */
  public void handleReviewAndFlexApplicationSize(ReviewAndFlexApplicationSize action)
      throws SliderInternalStateException {

    if ( actionQueues.hasQueuedActionWithAttribute(
        AsyncAction.ATTR_REVIEWS_APP_SIZE | AsyncAction.ATTR_HALTS_APP)) {
      // this operation isn't needed at all -existing duplicate or shutdown due
      return;
    }
    // if there is an action which changes cluster size, wait
    if (actionQueues.hasQueuedActionWithAttribute(
        AsyncAction.ATTR_CHANGES_APP_SIZE)) {
      // place the action at the back of the queue
      actionQueues.put(action);
    }
    
    executeNodeReview(action.name);
  }
  
  /**
   * Look at where the current node state is -and whether it should be changed
   */
  public synchronized void executeNodeReview(String reason)
      throws SliderInternalStateException {
    
    log.debug("in executeNodeReview({})", reason);
    if (amCompletionFlag.get()) {
      log.info("Ignoring node review operation: shutdown in progress");
    }
    try {
      List<AbstractRMOperation> allOperations = appState.reviewRequestAndReleaseNodes();
      // tell the provider
      providerRMOperationHandler.execute(allOperations);
      //now apply the operations
      executeRMOperations(allOperations);
    } catch (TriggerClusterTeardownException e) {

      //App state has decided that it is time to exit
      log.error("Cluster teardown triggered %s", e);
      signalAMComplete(e.getExitCode(), e.toString());
    }
  }
  
  
  
  
  /**
   * Shutdown operation: release all containers
   */
  private void releaseAllContainers() {
    //now apply the operations
    executeRMOperations(appState.releaseAllContainers());
  }

  /**
   * RM wants to shut down the AM
   */
  @Override //AMRMClientAsync
  public void onShutdownRequest() {
    LOG_YARN.info("Shutdown Request received");
    signalAMComplete(EXIT_CLIENT_INITIATED_SHUTDOWN, "Shutdown requested from RM");
  }

  /**
   * Monitored nodes have been changed
   * @param updatedNodes list of updated nodes
   */
  @Override //AMRMClientAsync
  public void onNodesUpdated(List<NodeReport> updatedNodes) {
    LOG_YARN.info("Nodes updated");
  }

  /**
   * heartbeat operation; return the ratio of requested
   * to actual
   * @return progress
   */
  @Override //AMRMClientAsync
  public float getProgress() {
    return appState.getApplicationProgressPercentage();
  }

  @Override //AMRMClientAsync
  public void onError(Throwable e) {
    //callback says it's time to finish
    LOG_YARN.error("AMRMClientAsync.onError() received " + e, e);
    signalAMComplete(EXIT_EXCEPTION_THROWN,
        "AMRMClientAsync.onError() received " + e);
  }
  
/* =================================================================== */
/* SliderClusterProtocol */
/* =================================================================== */

  @Override   //SliderClusterProtocol
  public ProtocolSignature getProtocolSignature(String protocol,
                                                long clientVersion,
                                                int clientMethodsHash) throws
                                                                       IOException {
    return ProtocolSignature.getProtocolSignature(
      this, protocol, clientVersion, clientMethodsHash);
  }



  @Override   //SliderClusterProtocol
  public long getProtocolVersion(String protocol, long clientVersion) throws
                                                                      IOException {
    return SliderClusterProtocol.versionID;
  }

  
/* =================================================================== */
/* SliderClusterProtocol */
/* =================================================================== */

  @Override //SliderClusterProtocol
  public Messages.StopClusterResponseProto stopCluster(Messages.StopClusterRequestProto request) throws
                                                                                                 IOException,
                                                                                                 YarnException {
    SliderUtils.getCurrentUser();
    String message = request.getMessage();
    log.info("SliderAppMasterApi.stopCluster: {}", message);
    schedule(new ActionStopSlider(message, 1000, TimeUnit.MILLISECONDS));
    return Messages.StopClusterResponseProto.getDefaultInstance();
  }

  @Override //SliderClusterProtocol
  public Messages.FlexClusterResponseProto flexCluster(Messages.FlexClusterRequestProto request) throws
                                                                                                 IOException,
                                                                                                 YarnException {
    SliderUtils.getCurrentUser();

    String payload = request.getClusterSpec();
    ConfTreeSerDeser confTreeSerDeser = new ConfTreeSerDeser();
    ConfTree updatedResources = confTreeSerDeser.fromJson(payload);
    flexCluster(updatedResources);
    return Messages.FlexClusterResponseProto.newBuilder().setResponse(true).build();
  }

  @Override //SliderClusterProtocol
  public Messages.GetJSONClusterStatusResponseProto getJSONClusterStatus(
    Messages.GetJSONClusterStatusRequestProto request) throws
                                                       IOException,
                                                       YarnException {
    SliderUtils.getCurrentUser();
    String result;
    //quick update
    //query and json-ify
    ClusterDescription cd = updateClusterStatus();
    result = cd.toJsonString();
    String stat = result;
    return Messages.GetJSONClusterStatusResponseProto.newBuilder()
      .setClusterSpec(stat)
      .build();
  }


  @Override
  public Messages.GetInstanceDefinitionResponseProto getInstanceDefinition(
    Messages.GetInstanceDefinitionRequestProto request) throws
                                                        IOException,
                                                        YarnException {

    log.info("Received call to getInstanceDefinition()");
    String internal;
    String resources;
    String app;
    synchronized (appState) {
      AggregateConf instanceDefinition = appState.getInstanceDefinition();
      internal = instanceDefinition.getInternal().toJson();
      resources = instanceDefinition.getResources().toJson();
      app = instanceDefinition.getAppConf().toJson();
    }
    assert internal != null;
    assert resources != null;
    assert app != null;
    log.info("Generating getInstanceDefinition Response");
    Messages.GetInstanceDefinitionResponseProto.Builder builder =
      Messages.GetInstanceDefinitionResponseProto.newBuilder();
    builder.setInternal(internal);
    builder.setResources(resources);
    builder.setApplication(app);
    return builder.build();
  }


  @Override //SliderClusterProtocol
  public Messages.ListNodeUUIDsByRoleResponseProto listNodeUUIDsByRole(Messages.ListNodeUUIDsByRoleRequestProto request) throws
                                                                                                                         IOException,
                                                                                                                         YarnException {
    SliderUtils.getCurrentUser();
    String role = request.getRole();
    Messages.ListNodeUUIDsByRoleResponseProto.Builder builder =
      Messages.ListNodeUUIDsByRoleResponseProto.newBuilder();
    List<RoleInstance> nodes = appState.enumLiveNodesInRole(role);
    for (RoleInstance node : nodes) {
      builder.addUuid(node.id);
    }
    return builder.build();
  }

  @Override //SliderClusterProtocol
  public Messages.GetNodeResponseProto getNode(Messages.GetNodeRequestProto request) throws
                                                                                     IOException,
                                                                                     YarnException {
    SliderUtils.getCurrentUser();
    RoleInstance instance = appState.getLiveInstanceByContainerID(
      request.getUuid());
    return Messages.GetNodeResponseProto.newBuilder()
                   .setClusterNode(instance.toProtobuf())
                   .build();
  }

  @Override //SliderClusterProtocol
  public Messages.GetClusterNodesResponseProto getClusterNodes(Messages.GetClusterNodesRequestProto request) throws
                                                                                                             IOException,
                                                                                                             YarnException {
    SliderUtils.getCurrentUser();
    List<RoleInstance>
      clusterNodes = appState.getLiveInstancesByContainerIDs(
      request.getUuidList());

    Messages.GetClusterNodesResponseProto.Builder builder =
      Messages.GetClusterNodesResponseProto.newBuilder();
    for (RoleInstance node : clusterNodes) {
      builder.addClusterNode(node.toProtobuf());
    }
    //at this point: a possibly empty list of nodes
    return builder.build();
  }

  @Override
  public Messages.EchoResponseProto echo(Messages.EchoRequestProto request) throws
                                                                            IOException,
                                                                            YarnException {
    Messages.EchoResponseProto.Builder builder =
      Messages.EchoResponseProto.newBuilder();
    String text = request.getText();
    log.info("Echo request size ={}", text.length());
    log.info(text);
    //now return it
    builder.setText(text);
    return builder.build();
  }

  @Override
  public Messages.KillContainerResponseProto killContainer(Messages.KillContainerRequestProto request) throws
                                                                                                       IOException,
                                                                                                       YarnException {
    String containerID = request.getId();
    log.info("Kill Container {}", containerID);
    //throws NoSuchNodeException if it is missing
    RoleInstance instance =
      appState.getLiveInstanceByContainerID(containerID);
    queue(new ActionKillContainer(instance.getId(), 0, TimeUnit.MILLISECONDS,
        rmOperationHandler));
    Messages.KillContainerResponseProto.Builder builder =
      Messages.KillContainerResponseProto.newBuilder();
    builder.setSuccess(true);
    return builder.build();
  }

  public void executeRMOperations(List<AbstractRMOperation> operations) {
    rmOperationHandler.execute(operations);
  }

  /**
   * Get the RM operations handler for direct scheduling of work.
   */
  @VisibleForTesting
  public RMOperationHandler getRmOperationHandler() {
    return rmOperationHandler;
  }

  @Override
  public Messages.AMSuicideResponseProto amSuicide(
      Messages.AMSuicideRequestProto request)
      throws IOException, YarnException {
    int signal = request.getSignal();
    String text = request.getText();
    int delay = request.getDelay();
    log.info("AM Suicide with signal {}, message {} delay = {}", signal, text, delay);
    ActionHalt action = new ActionHalt(signal, text, delay,
        TimeUnit.MILLISECONDS);
    schedule(action);
    return Messages.AMSuicideResponseProto.getDefaultInstance();
  }

  /* =================================================================== */
/* END */
/* =================================================================== */

  /**
   * Update the cluster description with anything interesting
   */
  public synchronized ClusterDescription updateClusterStatus() {
    Map<String, String> providerStatus = providerService.buildProviderStatus();
    assert providerStatus != null : "null provider status";
    return appState.refreshClusterStatus(providerStatus);
  }

  /**
   * Launch the provider service
   *
   * @param instanceDefinition definition of the service
   * @param confDir directory of config data
   * @throws IOException
   * @throws SliderException
   */
  protected synchronized void launchProviderService(AggregateConf instanceDefinition,
                                                    File confDir)
    throws IOException, SliderException {
    Map<String, String> env = new HashMap<String, String>();
    boolean execStarted = providerService.exec(instanceDefinition, confDir, env, this);
    if (execStarted) {
      providerService.registerServiceListener(this);
      providerService.start();
    } else {
      // didn't start, so don't register
      providerService.start();
      // and send the started event ourselves
      eventCallbackEvent(null);
    }
  }


  /* =================================================================== */
  /* EventCallback  from the child or ourselves directly */
  /* =================================================================== */

  @Override // ProviderCompleted
  public void eventCallbackEvent(Object parameter) {
    // signalled that the child process is up.
    appState.noteAMLive();
    // now ask for the cluster nodes
    try {
      flexCluster(getInstanceDefinition().getResources());
    } catch (Exception e) {
      //this may happen in a separate thread, so the ability to act is limited
      log.error("Failed to flex cluster nodes", e);
      //declare a failure
      finish();
    }
  }

  /**
   * report container loss. If this isn't already known about, react
   *
   * @param containerId       id of the container which has failed
   * @throws SliderException
   */
  public synchronized void providerLostContainer(
      ContainerId containerId)
      throws SliderException {
    log.info("containerLostContactWithProvider: container {} lost",
        containerId);
    RoleInstance activeContainer = appState.getOwnedContainer(containerId);
    if (activeContainer != null) {
      executeRMOperations(appState.releaseContainer(containerId));
      // ask for more containers if needed
      log.info("Container released; triggering review");
      reviewRequestAndReleaseNodes("Loss of container");
    } else {
      log.info("Container not in active set - ignoring");
    }
  }

  /* =================================================================== */
  /* ServiceStateChangeListener */
  /* =================================================================== */

  /**
   * Received on listening service termination.
   * @param service the service that has changed.
   */
  @Override //ServiceStateChangeListener
  public void stateChanged(Service service) {
    if (service == providerService && service.isInState(STATE.STOPPED)) {
      //its the current master process in play
      int exitCode = providerService.getExitCode();
      int mappedProcessExitCode =
        AMUtils.mapProcessExitCodeToYarnExitCode(exitCode);
      boolean shouldTriggerFailure = !amCompletionFlag.get()
         && (AMUtils.isMappedExitAFailure(mappedProcessExitCode));
      
      if (shouldTriggerFailure) {
        //this wasn't expected: the process finished early
        spawnedProcessExitedBeforeShutdownTriggered = true;
        log.info(
          "Process has exited with exit code {} mapped to {} -triggering termination",
          exitCode,
          mappedProcessExitCode);

        //tell the AM the cluster is complete 
        signalAMComplete(mappedProcessExitCode,
                         "Spawned master exited with raw " + exitCode + " mapped to " +
          mappedProcessExitCode);
      } else {
        //we don't care
        log.info(
          "Process has exited with exit code {} mapped to {} -ignoring",
          exitCode,
          mappedProcessExitCode);
      }
    } else {
      super.stateChanged(service);
    }
  }

  /**
   * stop forked process if it the running process var is not null
   * @return the process exit code
   */
  protected synchronized Integer stopForkedProcess() {
    providerService.stop();
    return providerService.getExitCode();
  }

  /**
   *  Async start container request
   * @param container container
   * @param ctx context
   * @param instance node details
   */
  public void startContainer(Container container,
                             ContainerLaunchContext ctx,
                             RoleInstance instance) {
    // Set up tokens for the container too. Today, for normal shell commands,
    // the container in distribute-shell doesn't need any tokens. We are
    // populating them mainly for NodeManagers to be able to download any
    // files in the distributed file-system. The tokens are otherwise also
    // useful in cases, for e.g., when one is running a "hadoop dfs" command
    // inside the distributed shell.
    ctx.setTokens(allTokens.duplicate());
    appState.containerStartSubmitted(container, instance);
    nmClientAsync.startContainerAsync(container, ctx);
  }

  @Override //  NMClientAsync.CallbackHandler 
  public void onContainerStopped(ContainerId containerId) {
    // do nothing but log: container events from the AM
    // are the source of container halt details to react to
    log.info("onContainerStopped {} ", containerId);
  }

  @Override //  NMClientAsync.CallbackHandler 
  public void onContainerStarted(ContainerId containerId,
                                 Map<String, ByteBuffer> allServiceResponse) {
    LOG_YARN.info("Started Container {} ", containerId);
    RoleInstance cinfo = appState.onNodeManagerContainerStarted(containerId);
    if (cinfo != null) {
      LOG_YARN.info("Deployed instance of role {} onto {}",
          cinfo.role, containerId);
      //trigger an async container status
      nmClientAsync.getContainerStatusAsync(containerId,
                                            cinfo.container.getNodeId());
      // push out a registration
      queue(new RegisterComponentInstance(containerId, 0, TimeUnit.MILLISECONDS));
      
    } else {
      //this is a hypothetical path not seen. We react by warning
      log.error("Notified of started container that isn't pending {} - releasing",
                containerId);
      //then release it
      asyncRMClient.releaseAssignedContainer(containerId);
    }
  }

  @Override //  NMClientAsync.CallbackHandler 
  public void onStartContainerError(ContainerId containerId, Throwable t) {
    LOG_YARN.error("Failed to start Container " + containerId, t);
    appState.onNodeManagerContainerStartFailed(containerId, t);
  }

  @Override //  NMClientAsync.CallbackHandler 
  public void onContainerStatusReceived(ContainerId containerId,
                                        ContainerStatus containerStatus) {
    LOG_YARN.debug("Container Status: id={}, status={}", containerId,
        containerStatus);
  }

  @Override //  NMClientAsync.CallbackHandler 
  public void onGetContainerStatusError(
    ContainerId containerId, Throwable t) {
    LOG_YARN.error("Failed to query the status of Container {}", containerId);
  }

  @Override //  NMClientAsync.CallbackHandler 
  public void onStopContainerError(ContainerId containerId, Throwable t) {
    LOG_YARN.warn("Failed to stop Container {}", containerId);
  }

  /**
   The cluster description published to callers
   This is used as a synchronization point on activities that update
   the CD, and also to update some of the structures that
   feed in to the CD
   */
  public ClusterDescription getClusterSpec() {
    return appState.getClusterSpec();
  }

  public AggregateConf getInstanceDefinition() {
    return appState.getInstanceDefinition();
  }

  /**
   * This is the status, the live model
   */
  public ClusterDescription getClusterDescription() {
    return appState.getClusterStatus();
  }

  public ProviderService getProviderService() {
    return providerService;
  }

  /**
   * Queue an action for immediate execution in the executor thread
   * @param action action to execute
   */
  public void queue(AsyncAction action) {
    actionQueues.put(action);
  }

  /**
   * Schedule an action
   * @param action for delayed execution
   */
  public void schedule(AsyncAction action) {
    actionQueues.schedule(action);
  }


  /**
   * Handle any exception in a thread. If the exception provides an exit
   * code, that is the one that will be used
   * @param thread thread throwing the exception
   * @param exception exception
   */
  public void onExceptionInThread(Thread thread, Exception exception) {
    log.error("Exception in {}: {}", thread.getName(), exception, exception);
    int exitCode = EXIT_EXCEPTION_THROWN;
    if (exception instanceof ExitCodeProvider) {
      exitCode = ((ExitCodeProvider) exception).getExitCode();
    }
    signalAMComplete(exitCode, exception.toString());
  }

  /**
   * Start the chaos monkey
   * @return true if it started
   */
  private boolean maybeStartMonkey() {
    MapOperations internals = getGlobalInternalOptions();

    Boolean enabled =
        internals.getOptionBool(InternalKeys.CHAOS_MONKEY_ENABLED,
            InternalKeys.DEFAULT_CHAOS_MONKEY_ENABLED);
    if (!enabled) {
      log.info("Chaos monkey disabled");
    }
    
    long monkeyInterval = internals.getTimeRange(
        InternalKeys.CHAOS_MONKEY_INTERVAL,
        InternalKeys.DEFAULT_CHAOS_MONKEY_INTERVAL_DAYS,
        InternalKeys.DEFAULT_CHAOS_MONKEY_INTERVAL_HOURS,
        InternalKeys.DEFAULT_CHAOS_MONKEY_INTERVAL_MINUTES,
        0);
    if (monkeyInterval == 0) {
      log.debug(
          "Chaos monkey not configured with a time interval...not enabling");
      return false;
    }
    log.info("Adding Chaos Monkey scheduled every {} seconds ({} hours)",
        monkeyInterval, monkeyInterval/(60*60));
    monkey = new ChaosMonkeyService(metrics, actionQueues);
    initAndAddService(monkey);
    
    // configure the targets
    int amKillProbability = internals.getOptionInt(
        InternalKeys.CHAOS_MONKEY_PROBABILITY_AM_FAILURE,
        InternalKeys.DEFAULT_CHAOS_MONKEY_PROBABILITY_AM_FAILURE);
    monkey.addTarget("AM killer",
        new ChaosKillAM(actionQueues, -1), amKillProbability);
    int containerKillProbability = internals.getOptionInt(
        InternalKeys.CHAOS_MONKEY_PROBABILITY_CONTAINER_FAILURE,
        InternalKeys.DEFAULT_CHAOS_MONKEY_PROBABILITY_CONTAINER_FAILURE);
    monkey.addTarget("Container killer",
        new ChaosKillContainer(appState, actionQueues, rmOperationHandler),
        containerKillProbability);
    
    // and schedule it
    if (monkey.schedule(monkeyInterval, TimeUnit.SECONDS)) {
      log.info("Chaos Monkey is running");
      return true;
    } else {
      log.info("Chaos monkey not started");
      return false;
    }
  }
  
  /**
   * This is the main entry point for the service launcher.
   * @param args command line arguments.
   */
  public static void main(String[] args) {

    //turn the args to a list
    List<String> argsList = Arrays.asList(args);
    //create a new list, as the ArrayList type doesn't push() on an insert
    List<String> extendedArgs = new ArrayList<String>(argsList);
    //insert the service name
    extendedArgs.add(0, SERVICE_CLASSNAME);
    //now have the service launcher do its work
    ServiceLauncher.serviceMain(extendedArgs);
  }

}
