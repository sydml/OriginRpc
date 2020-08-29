package com.sydml.rpc.zookeeper;

import com.google.common.collect.Lists;
import com.sydml.rpc.framework.helper.IPHelper;
import com.sydml.rpc.framework.helper.PropertyConfigeHelper;
import com.sydml.rpc.framework.model.InvokerService;
import com.sydml.rpc.framework.model.ProviderService;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.GetDataBuilder;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class RegisterCenter implements IRegisterCenterInvoker, IRegisterCenterProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(RegisterCenter.class);

    public static final int BASE_SLEEP_TIME_MS = PropertyConfigeHelper.getBaseSleepTimeMs();
    public static final int MAX_RETRIES = PropertyConfigeHelper.getMaxRetries();
    private static RegisterCenter registerCenter = new RegisterCenter();

    private static final Map<String, List<ProviderService>> providerServiceMap = new ConcurrentHashMap<>();

    private static final Map<String, List<ProviderService>> serviceMetaDataMapConsume = new ConcurrentHashMap<>();

    private static String ZK_SERVER = PropertyConfigeHelper.getZkService();

    private static int ZK_SESSION_TIME_OUT = PropertyConfigeHelper.getZkConnectionTimeout();
    private static int ZK_CONNECTION_TIME_OUT = PropertyConfigeHelper.getZkConnectionTimeout();
    private static String ROOT_PATH = "/config_register";
    public static String PROVIDER_TYPE = "provider";
    public static String INVOKER_TYPE = "consumer";
//    private static volatile ZkClient zkClient = null;
    private RetryPolicy retryPolicy;
    private static volatile CuratorFramework curatorClient;
    private RegisterCenter() {
    }

    public static RegisterCenter singleton() {
        return registerCenter;
    }

    @Override
    public void initProviderMap(String remoteAppKey, String groupName) {
        if (MapUtils.isEmpty(serviceMetaDataMapConsume)) {
            serviceMetaDataMapConsume.putAll(fetchOrUpdateServiceMetaData(remoteAppKey, groupName));
        }
    }



    @Override
    public Map<String, List<ProviderService>> getServiceMetaDataMapConsume() {
        return serviceMetaDataMapConsume;
    }

    @Override
    public void registerInvoker(InvokerService invoker) {
        if (invoker == null) {
            return;
        }
        synchronized (RegisterCenter.class){
            if (curatorClient == null) {
                retryPolicy = new ExponentialBackoffRetry(BASE_SLEEP_TIME_MS, MAX_RETRIES);
                curatorClient = CuratorFrameworkFactory.builder().connectString(ZK_SERVER).sessionTimeoutMs(ZK_SESSION_TIME_OUT).connectionTimeoutMs(ZK_CONNECTION_TIME_OUT).retryPolicy(retryPolicy).build();
                curatorClient.start();
            }
            try {
                Stat stat = curatorClient.checkExists().forPath(ROOT_PATH);
                if (stat == null) {
                    curatorClient.create().withMode(CreateMode.PERSISTENT).forPath(ROOT_PATH);
                }
                String remoteKey = invoker.getRemoteAppKey();
                String groupName = invoker.getGroupName();
                String serviceNode = invoker.getServiceInterface().getName();
                String servicePath = ROOT_PATH + "/" + remoteKey + groupName + "/" + serviceNode + "/" + INVOKER_TYPE;
                stat = curatorClient.checkExists().forPath(servicePath);
                if (stat == null) {
                    curatorClient.create().withMode(CreateMode.PERSISTENT).forPath(servicePath);
                }
                String localIp = IPHelper.localIp();
                String currentServiceIpNode = servicePath + "/" + localIp;
                stat = curatorClient.checkExists().forPath(currentServiceIpNode);
                if (stat == null) {
                    curatorClient.create().withMode(CreateMode.EPHEMERAL).forPath(currentServiceIpNode);
                }
            } catch (Exception e) {
                LOGGER.error("zk error", e);
            }
        }
    }

    @Override
    public void registerProvider(List<ProviderService> serviceMetaData) {
        if (CollectionUtils.isEmpty(serviceMetaData)) {
            return;
        }
        synchronized (RegisterCenter.class) {
            for (ProviderService provider : serviceMetaData) {
                String serviceInterfaceKey = provider.getServiceInterface().getName();
                List<ProviderService> providers = providerServiceMap.get(serviceInterfaceKey);
                if (providers == null) {
                    providers = new ArrayList<>();
                }
                providers.add(provider);
                providerServiceMap.put(serviceInterfaceKey, providers);
            }
            if (curatorClient == null) {
                retryPolicy = new ExponentialBackoffRetry(BASE_SLEEP_TIME_MS, MAX_RETRIES);
                curatorClient = CuratorFrameworkFactory.builder().connectString(ZK_SERVER).sessionTimeoutMs(ZK_SESSION_TIME_OUT).connectionTimeoutMs(ZK_CONNECTION_TIME_OUT).retryPolicy(retryPolicy).build();
                curatorClient.start();
            }
            try {
                Stat stat = curatorClient.checkExists().forPath(ROOT_PATH);
                if (stat == null) {
                    curatorClient.create().withMode(CreateMode.PERSISTENT).forPath(ROOT_PATH);
                }
                for (Map.Entry<String, List<ProviderService>> entry : providerServiceMap.entrySet()) {
                    String serviceNode = entry.getKey();
                    String servicePath = ROOT_PATH + "/" + serviceNode + PROVIDER_TYPE;
                    stat = curatorClient.checkExists().forPath(servicePath);
                    if (stat == null) {
                        curatorClient.create().withMode(CreateMode.PERSISTENT).forPath(servicePath);
                    }
                    int serverPort = entry.getValue().get(0).getServerPort();
                    String localIp = IPHelper.localIp();
                    String currentServiceIpNode = servicePath + "/" + localIp + "|" + serverPort;
                    stat = curatorClient.checkExists().forPath(currentServiceIpNode);
                    if (stat == null) {
                        curatorClient.create().withMode(CreateMode.EPHEMERAL).forPath(currentServiceIpNode);
                    }
                    PathChildrenCache pathChildrenCache = new PathChildrenCache(curatorClient, servicePath, false);
                    pathChildrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
                    List<ChildData> childDataList = pathChildrenCache.getCurrentData();
                    for (ChildData cd : childDataList) {
                        String childData = new String(cd.getData());
                        LOGGER.info("子节点数据变化：" + childData);
                    }
                    pathChildrenCache.getListenable().addListener((client, event) -> {
                                List<String> activityServiceIpLists;
                                if (event.getData().getData() == null) {
                                    activityServiceIpLists = new ArrayList<>();
                                } else {
                                    String activityServiceIp = new String(event.getData().getData()).split("|")[0];
                                    activityServiceIpLists = Arrays.asList(activityServiceIp);
                                }
                                refreshActivityService(activityServiceIpLists);
                            }
                    );

                /*curatorClient.subscribeChildChanges(servicePath, new IZkChildListener() {
                    @Override
                    public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
                        if (currentChilds == null) {
                            currentChilds = new ArrayList<>();
                        }
                        List<String> activityServiceIpList = currentChilds.stream().map(input -> StringUtils.split(input, "|")[0]).collect(Collectors.toList());
                        refreshActivityService(activityServiceIpList);
                    }
                });*/
                }
            } catch (Exception e) {
                LOGGER.error("zk error", e);
            }

        }
    }

    @Override
    public Map<String, List<ProviderService>> getProviderServiceMap() {
        return providerServiceMap;
    }

    private Map<String, List<ProviderService>> fetchOrUpdateServiceMetaData(String remoteAppKey, String groupName) {
        final Map<String, List<ProviderService>> providerServiceMap = new ConcurrentHashMap<>();
        synchronized (RegisterCenter.class){
            if (curatorClient == null) {
                retryPolicy = new ExponentialBackoffRetry(BASE_SLEEP_TIME_MS, MAX_RETRIES);
                curatorClient = CuratorFrameworkFactory.builder().connectString(ZK_SERVER).sessionTimeoutMs(ZK_SESSION_TIME_OUT).connectionTimeoutMs(ZK_CONNECTION_TIME_OUT).retryPolicy(retryPolicy).build();
                curatorClient.start();
            }
        }
        String providerPath = ROOT_PATH +"/" +remoteAppKey + "/" +groupName;
        try {
            List<String> providerServices = curatorClient.getChildren().forPath(providerPath);
            for (String serviceName : providerServices) {
                String servicePath = providerPath + "/" + serviceName + "/" + PROVIDER_TYPE;
                List<String> ipPathList = curatorClient.getChildren().forPath(servicePath);
                for (String ipPath : ipPathList) {
                    String serverIp = StringUtils.split(ipPath, "|")[0];
                    String serverPort = StringUtils.split(ipPath, "|")[1];
                    int weight = Integer.parseInt(StringUtils.split(ipPath, "|")[2]);
                    int workThreads = Integer.parseInt(StringUtils.split(ipPath, "|")[3]);
                    String group = StringUtils.split(ipPath, "|")[4];
                    List<ProviderService> providerServiceList = providerServiceMap.get(serviceName);
                    if (providerServiceList == null) {
                        providerServiceList = new ArrayList<>();
                    }
                    ProviderService providerService = new ProviderService();
                    try {
                        providerService.setServiceInterface(ClassUtils.getClass(serviceName));
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                    providerService.setServerIp(serverIp);
                    providerService.setServerPort(Integer.parseInt(serverPort));
                    providerService.setWeight(weight);
                    providerService.setWorkerThreads(workThreads);
                    providerService.setGroupName(groupName);
                    providerService.setAppKey(remoteAppKey);
                    providerServiceList.add(providerService);
                    providerServiceMap.put(serviceName, providerServiceList);
                }


                PathChildrenCache pathChildrenCache = new PathChildrenCache(curatorClient, servicePath, false);
                pathChildrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
                List<ChildData> childDataList = pathChildrenCache.getCurrentData();
                for (ChildData cd : childDataList) {
                    String childData = new String(cd.getData());
                    LOGGER.info("子节点数据变化：" + childData);
                }
                pathChildrenCache.getListenable().addListener((client, event) -> {
                            List<String> activityServiceIpLists;
                            if (event.getData().getData() == null) {
                                activityServiceIpLists = new ArrayList<>();
                            } else {
                                String activityServiceIp = new String(event.getData().getData()).split("|")[0];
                                activityServiceIpLists = Arrays.asList(activityServiceIp);
                            }
                            refreshServiceMetaDataMap(activityServiceIpLists);
                        }
                );


           /* zkClient.subscribeChildChanges(servicePath, new IZkChildListener() {
                @Override
                public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
                    if (currentChilds == null) {
                        currentChilds = new ArrayList<>();
                    }
                    currentChilds = currentChilds.stream().map(input -> StringUtils.split(input, "|")[0]).collect(Collectors.toList());
                    refreshServiceMetaDataMap(currentChilds);
                }
            });*/
            }
        }catch (Exception e){
            LOGGER.error("zk error", e);
        }
        return providerServiceMap;
    }

    private void refreshServiceMetaDataMap(List<String> serviceIpList) {
        if (serviceIpList == null) {
            serviceIpList = new ArrayList<>();
        }
        Map<String,List<ProviderService>> currentServiceMetaDataMap = new HashMap<>();
        for (Map.Entry<String, List<ProviderService>> entry : serviceMetaDataMapConsume.entrySet()) {
            String serviceInterfaceKey = entry.getKey();
            List<ProviderService> serviceList = entry.getValue();
            List<ProviderService> providerServiceList = currentServiceMetaDataMap.get(serviceInterfaceKey);

            if (providerServiceList == null) {
                providerServiceList = new ArrayList<>();
            }
            for (ProviderService providerServiceMetaData : serviceList) {
                if (serviceIpList.contains(providerServiceMetaData.getServerIp())) {
                    providerServiceList.add(providerServiceMetaData);
                }
            }
            currentServiceMetaDataMap.put(serviceInterfaceKey, providerServiceList);
        }
        serviceMetaDataMapConsume.clear();
        serviceMetaDataMapConsume.putAll(currentServiceMetaDataMap);
    }

    private void refreshActivityService(List<String> serviceIpList) {
        if (serviceIpList == null) {
            serviceIpList = new ArrayList<>();
        }
        Map<String, List<ProviderService>> currentServiceMetaDataMap = new HashMap<>();
        for (Map.Entry<String, List<ProviderService>> entry : providerServiceMap.entrySet()) {
            String key = entry.getKey();
            List<ProviderService> providerServices = entry.getValue();
            List<ProviderService> serviceMetaDataModelList = currentServiceMetaDataMap.get(key);
            if (serviceMetaDataModelList == null) {
                serviceMetaDataModelList = new ArrayList<>();
            }
            for (ProviderService serviceMetaData : providerServices) {
                if (serviceIpList.contains(serviceMetaData)) {
                    serviceMetaDataModelList.add(serviceMetaData);
                }
            }
            currentServiceMetaDataMap.put(key, serviceMetaDataModelList);
        }
        providerServiceMap.clear();
        providerServiceMap.putAll(currentServiceMetaDataMap);
    }
}
