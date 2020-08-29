package com.sydml.rpc.framework.model;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;


public class RpcResponseWrapper {

    //存储返回结果的阻塞队列
    private BlockingQueue<RpcResponse> responseQueue = new ArrayBlockingQueue<RpcResponse>(1);
    //结果返回时间
    private long responseTime;

    /**
     * 计算该返回结果是否已经过期
     *
     * @return
     */
    public boolean isExpire() {
        RpcResponse response = responseQueue.peek();
        if (response == null) {
            return false;
        }

        long timeout = response.getInvokeTimeout();
        if ((System.currentTimeMillis() - responseTime) > timeout) {
            return true;
        }
        return false;
    }

    public static RpcResponseWrapper of() {
        return new RpcResponseWrapper();
    }

    public BlockingQueue<RpcResponse> getResponseQueue() {
        return responseQueue;
    }

    public long getResponseTime() {
        return responseTime;
    }

    public void setResponseTime(long responseTime) {
        this.responseTime = responseTime;
    }
}
