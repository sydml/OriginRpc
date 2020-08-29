package com.sydml.rpc.zookeeper.demo;

import java.io.Serializable;

/**
 * @author liuyuming
 * @date 2020-07-27 20:25:47
 */
public class ServerData implements Serializable {

    private int serverId;

    private String serverName;

    public int getServerId() {
        return serverId;
    }

    public void setServerId(int serverId) {
        this.serverId = serverId;
    }

    public String getServerName() {
        return serverName;
    }

    public void setServerName(String serverName) {
        this.serverName = serverName;
    }
}
