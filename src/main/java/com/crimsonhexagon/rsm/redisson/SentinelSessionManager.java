package com.crimsonhexagon.rsm.redisson;

import org.redisson.config.Config;
import org.redisson.config.ReadMode;
import org.redisson.config.SentinelServersConfig;
import org.redisson.connection.balancer.LoadBalancer;
import org.redisson.connection.balancer.RoundRobinLoadBalancer;

public class SentinelSessionManager extends BaseRedissonSessionManager {
    public static final String DEFAULT_LOAD_BALANCER_CLASS = RoundRobinLoadBalancer.class.getName();
    public static final String DEFAULT_MASTER_NAME = "mymaster";
    public static final int DEFAULT_MASTER_CONN_POOL_SIZE = 100;
    public static final int DEFAULT_SLAVE_CONN_POOL_SIZE = 100;

    private String nodes;
    private String masterName;
    private String loadBalancerClass = DEFAULT_LOAD_BALANCER_CLASS;
    private int masterConnectionPoolSize = DEFAULT_MASTER_CONN_POOL_SIZE;
    private int slaveConnectionPoolSize = DEFAULT_SLAVE_CONN_POOL_SIZE;

    @Override
    protected Config configure(Config config) {
        if (masterName == null) {
            masterName = DEFAULT_MASTER_NAME;
            log.warn(String.format("Master name is empty. Using default name: {}", masterName));
        }

        if (nodes == null || nodes.trim().length() == 0) {
            throw new IllegalStateException("Manager must specify sentinel node string. e.g., nodes=\"node1.com:26389 node2.com:26389\"");
        }
        LoadBalancer lb = null;
        if (loadBalancerClass != null && loadBalancerClass.trim().length() != 0) {
            try {
                lb = LoadBalancer.class.cast(Class.forName(loadBalancerClass).newInstance());
            } catch (Exception e) {
                log.error("Failed to instantiate LoadBalancer", e);
            }
        }

        SentinelServersConfig ssCfg = config.useSentinelServers();
        ssCfg
            .setMasterName(masterName)
            .addSentinelAddress(nodes.trim().split("\\s+"))
            .setDatabase(database)
            .setMasterConnectionPoolSize(masterConnectionPoolSize)
            .setSlaveConnectionPoolSize(slaveConnectionPoolSize)
            .setPassword(password)
            .setTimeout(timeout)
            .setReadMode(ReadMode.MASTER_SLAVE)
            .setPingTimeout(pingTimeout)
            .setRetryAttempts(retryAttempts)
            .setRetryInterval(retryInterval);
        if (lb != null) {
            ssCfg.setLoadBalancer(lb);
        }
        return config;
    }

    public String getNodes() {
        return nodes;
    }

    public void setNodes(String nodes) {
        this.nodes = nodes;
    }

    public String getMasterName() {
        return masterName;
    }

    public void setMasterName(String masterName) {
        this.masterName = masterName;
    }

    public String getLoadBalancerClass() {
        return loadBalancerClass;
    }

    public void setLoadBalancerClass(String loadBalancerClass) {
        this.loadBalancerClass = loadBalancerClass;
    }

    public int getMasterConnectionPoolSize() {
        return masterConnectionPoolSize;
    }

    public void setMasterConnectionPoolSize(int masterConnectionPoolSize) {
        this.masterConnectionPoolSize = masterConnectionPoolSize;
    }

    public int getSlaveConnectionPoolSize() {
        return slaveConnectionPoolSize;
    }

    public void setSlaveConnectionPoolSize(int slaveConnectionPoolSize) {
        this.slaveConnectionPoolSize = slaveConnectionPoolSize;
    }

}
