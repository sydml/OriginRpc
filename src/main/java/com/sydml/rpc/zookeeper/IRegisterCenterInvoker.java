package com.sydml.rpc.zookeeper;

import com.sydml.rpc.framework.model.InvokerService;
import com.sydml.rpc.framework.model.ProviderService;

import java.util.List;
import java.util.Map;

public interface IRegisterCenterInvoker {
    /**
     * 初始化提供者
     * @param remoteAppKey
     * @param groupName
     */
    void initProviderMap(String remoteAppKey, String groupName);

    /**
     * 获取消费服务
     * @return
     */
    Map<String, List<ProviderService>> getServiceMetaDataMapConsume();

    /**
     * 注册调用者
     * @param invoker
     */
    void registerInvoker(final InvokerService invoker);
}
