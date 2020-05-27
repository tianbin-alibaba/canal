package com.alibaba.otter.canal.server;

import java.util.concurrent.TimeUnit;

import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.server.exception.CanalServerException;

public interface CanalService {

    void subscribe(ClientIdentity clientIdentity) throws CanalServerException;

    void unsubscribe(ClientIdentity clientIdentity) throws CanalServerException;
    //比例获取数据，并自动自行ack
    Message get(ClientIdentity clientIdentity, int batchSize) throws CanalServerException;
    //超时时间内批量获取数据，并自动进行ack
    Message get(ClientIdentity clientIdentity, int batchSize, Long timeout, TimeUnit unit) throws CanalServerException;
    //批量获取数据，不进行ack
    Message getWithoutAck(ClientIdentity clientIdentity, int batchSize) throws CanalServerException;
    //超时时间内批量获取数据，不进行ack
    Message getWithoutAck(ClientIdentity clientIdentity, int batchSize, Long timeout, TimeUnit unit)
                                                                                                    throws CanalServerException;
    //ack某个批次的数据
    void ack(ClientIdentity clientIdentity, long batchId) throws CanalServerException;
    //回滚所有没有ack的批次的数据
    void rollback(ClientIdentity clientIdentity) throws CanalServerException;
    //回滚某个批次的数据
    void rollback(ClientIdentity clientIdentity, Long batchId) throws CanalServerException;
}
