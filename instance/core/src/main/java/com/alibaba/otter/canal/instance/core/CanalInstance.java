package com.alibaba.otter.canal.instance.core;

import com.alibaba.otter.canal.common.CanalLifeCycle;
import com.alibaba.otter.canal.common.alarm.CanalAlarmHandler;
import com.alibaba.otter.canal.meta.CanalMetaManager;
import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.sink.CanalEventSink;
import com.alibaba.otter.canal.store.CanalEventStore;

/**
 * 代表单个canal实例，比如一个destination会独立一个实例
 * 
 * @author jianghang 2012-7-12 下午12:04:58
 * @version 1.0.0
 */
public interface CanalInstance extends CanalLifeCycle {
    //这个instance对应的destination
    String getDestination();
    //数据源接入，模拟slave协议和master进行交互，协议解析，位于canal.parse模块中
    CanalEventParser getEventParser();
    //parser和store链接器，进行数据过滤，加工，分发的工作，位于canal.sink模块中
    CanalEventSink getEventSink();
    //数据存储，位于canal.store模块中
    CanalEventStore getEventStore();
    //增量订阅&消费元数据管理器，位于canal.meta模块中
    CanalMetaManager getMetaManager();
    //告警，位于canal.common块中
    CanalAlarmHandler getAlarmHandler();

    /**
     * 客户端发生订阅/取消订阅行为
     */
    boolean subscribeChange(ClientIdentity identity);

    CanalMQConfig getMqConfig();
}
