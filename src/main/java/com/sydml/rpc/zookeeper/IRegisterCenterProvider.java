package com.sydml.rpc.zookeeper;

import com.sydml.rpc.framework.model.ProviderService;

import java.util.List;
import java.util.Map;

public interface IRegisterCenterProvider {

    /**
     * 注册提供者
     * @param serviceMetaData
     */
    void registerProvider(final List<ProviderService> serviceMetaData);

    /**
     * 获取提供者
     * @return
     */
    Map<String, List<ProviderService>> getProviderServiceMap();
}
