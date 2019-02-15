package com.alibaba.otter.canal.adapter.launcher.monitor;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.alibaba.otter.canal.client.adapter.support.Constant;
import com.alibaba.otter.canal.client.adapter.support.MappingConfigsLoader;
import com.alibaba.otter.canal.common.utils.NamedThreadFactory;
import com.google.common.base.Joiner;
import com.google.common.collect.MapMaker;

/**
 * 远程配置装载、监控类
 *
 * @author rewerma @ 2019-01-05
 * @version 1.0.0
 */
public class AdapterRemoteConfigMonitor {

    private static final Logger      logger                 = LoggerFactory.getLogger(AdapterRemoteConfigMonitor.class);

    private Connection               conn;
    private String                   jdbcUrl;
    private String                   jdbcUsername;
    private String                   jdbcPassword;

    private long                     currentConfigTimestamp = 0;
    private Map<String, ConfigItem>  remoteAdapterConfigs   = new MapMaker().makeMap();

    private ScheduledExecutorService executor               = Executors.newScheduledThreadPool(2,
        new NamedThreadFactory("remote-adapter-config-scan"));

    public AdapterRemoteConfigMonitor(String jdbcUrl, String jdbcUsername, String jdbcPassword){
        this.jdbcUrl = jdbcUrl;
        this.jdbcUsername = jdbcUsername;
        this.jdbcPassword = jdbcPassword;
    }

    private Connection getConn() throws Exception {
        if (conn == null || conn.isClosed()) {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);
        }
        return conn;
    }

    /**
     * 加载远程application.yml配置到本地
     */
    public void loadRemoteConfig() {
        try {
            // 加载远程adapter配置
            ConfigItem configItem = getRemoteAdapterConfig();
            if (configItem != null) {
                if (configItem.getModifiedTime() != currentConfigTimestamp) {
                    currentConfigTimestamp = configItem.getModifiedTime();
                    overrideLocalCanalConfig(configItem.getContent());
                    logger.info("## Loaded remote adapter config: application.yml");
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 获取远程application.yml配置
     *
     * @return 配置对象
     */
    private ConfigItem getRemoteAdapterConfig() {
        String sql = "select name, content, modified_time from canal_config where id=2";
        try (Statement stmt = getConn().createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                ConfigItem configItem = new ConfigItem();
                configItem.setId(2L);
                configItem.setName(rs.getString("name"));
                configItem.setContent(rs.getString("content"));
                configItem.setModifiedTime(rs.getTimestamp("modified_time").getTime());
                return configItem;
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return null;
    }

    /**
     * 覆盖本地application.yml文件
     *
     * @param content 文件内容
     */
    private void overrideLocalCanalConfig(String content) {
        try (FileWriter writer = new FileWriter(getConfPath() + "application.yml")) {
            writer.write(content);
            writer.flush();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 启动监听数据库变化
     */
    public void start() {
        // 监听application.yml变化
        executor.scheduleWithFixedDelay(() -> {
            try {
                loadRemoteConfig();
            } catch (Throwable e) {
                logger.error("scan remote application.yml failed", e);
            }
        }, 10, 3, TimeUnit.SECONDS);

        // 监听adapter变化
        executor.scheduleWithFixedDelay(() -> {
            try {
                loadRemoteAdapterConfigs();
            } catch (Throwable e) {
                logger.error("scan remote adapter configs failed", e);
            }
        }, 10, 3, TimeUnit.SECONDS);
    }

    /**
     * 加载adapter配置到本地
     */
    public void loadRemoteAdapterConfigs() {
        try {
            // 加载远程adapter配置
            Map<String, ConfigItem>[] modifiedConfigs = getModifiedAdapterConfigs();
            if (modifiedConfigs != null) {
                overrideLocalAdapterConfigs(modifiedConfigs);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 获取有变动的adapter配置
     *
     * @return Map[0]: 新增修改的配置, Map[1]: 删除的配置
     */
    @SuppressWarnings("unchecked")
    private Map<String, ConfigItem>[] getModifiedAdapterConfigs() {
        Map<String, ConfigItem>[] res = new Map[2];
        Map<String, ConfigItem> remoteConfigStatus = new HashMap<>();
        String sql = "select id, category, name, modified_time from canal_adapter_config";
        try (Statement stmt = getConn().createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                ConfigItem configItem = new ConfigItem();
                configItem.setId(rs.getLong("id"));
                configItem.setCategory(rs.getString("category"));
                configItem.setName(rs.getString("name"));
                configItem.setModifiedTime(rs.getTimestamp("modified_time").getTime());
                remoteConfigStatus.put(configItem.getCategory() + "/" + configItem.getName(), configItem);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return null;
        }

        if (!remoteConfigStatus.isEmpty()) {
            List<Long> changedIds = new ArrayList<>();

            for (ConfigItem remoteConfigStat : remoteConfigStatus.values()) {
                ConfigItem currentConfig = remoteAdapterConfigs
                    .get(remoteConfigStat.getCategory() + "/" + remoteConfigStat.getName());
                if (currentConfig == null) {
                    // 新增
                    changedIds.add(remoteConfigStat.getId());
                } else {
                    // 修改
                    if (currentConfig.getModifiedTime() != remoteConfigStat.getModifiedTime()) {
                        changedIds.add(remoteConfigStat.getId());
                    }
                }
            }
            if (!changedIds.isEmpty()) {
                Map<String, ConfigItem> changedAdapterConfig = new HashMap<>();
                String contentsSql = "select id, category, name, content, modified_time from canal_adapter_config  where id in ("
                                     + Joiner.on(",").join(changedIds) + ")";
                try (Statement stmt = getConn().createStatement(); ResultSet rs = stmt.executeQuery(contentsSql)) {
                    while (rs.next()) {
                        ConfigItem configItemNew = new ConfigItem();
                        configItemNew.setId(rs.getLong("id"));
                        configItemNew.setCategory(rs.getString("category"));
                        configItemNew.setName(rs.getString("name"));
                        configItemNew.setContent(rs.getString("content"));
                        configItemNew.setModifiedTime(rs.getTimestamp("modified_time").getTime());

                        remoteAdapterConfigs.put(configItemNew.getCategory() + "/" + configItemNew.getName(),
                            configItemNew);
                        changedAdapterConfig.put(configItemNew.getCategory() + "/" + configItemNew.getName(),
                            configItemNew);
                    }

                    res[0] = changedAdapterConfig.isEmpty() ? null : changedAdapterConfig;
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }

        Map<String, ConfigItem> removedAdapterConfig = new HashMap<>();
        for (ConfigItem configItem : remoteAdapterConfigs.values()) {
            if (!remoteConfigStatus.containsKey(configItem.getCategory() + "/" + configItem.getName())) {
                // 删除
                remoteAdapterConfigs.remove(configItem.getCategory() + "/" + configItem.getName());
                removedAdapterConfig.put(configItem.getCategory() + "/" + configItem.getName(), null);
            }
        }
        res[1] = removedAdapterConfig.isEmpty() ? null : removedAdapterConfig;

        if (res[0] == null && res[1] == null) {
            return null;
        } else {
            return res;
        }
    }

    /**
     * 覆盖adapter配置到本地
     *
     * @param modifiedAdapterConfigs 变动的配置集合
     */
    private void overrideLocalAdapterConfigs(Map<String, ConfigItem>[] modifiedAdapterConfigs) {
        Map<String, ConfigItem> changedAdapterConfigs = modifiedAdapterConfigs[0];
        if (changedAdapterConfigs != null) {
            for (ConfigItem configItem : changedAdapterConfigs.values()) {
                try (FileWriter writer = new FileWriter(
                    getConfPath() + configItem.getCategory() + "/" + configItem.getName())) {
                    writer.write(configItem.getContent());
                    writer.flush();
                    logger.info("## Loaded remote adapter config: {}/{}",
                        configItem.getCategory(),
                        configItem.getName());
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
        Map<String, ConfigItem> removedAdapterConfigs = modifiedAdapterConfigs[1];
        if (removedAdapterConfigs != null) {
            for (String name : removedAdapterConfigs.keySet()) {
                File file = new File(getConfPath() + name);
                if (file.exists()) {
                    deleteDir(file);
                    logger.info("## Deleted and reloaded remote adapter config: {}", name);
                }
            }
        }
    }

    private static boolean deleteDir(File dirFile) {
        if (!dirFile.exists()) {
            return false;
        }

        if (dirFile.isFile()) {
            return dirFile.delete();
        } else {
            File[] files = dirFile.listFiles();
            if (files == null || files.length == 0) {
                return dirFile.delete();
            }
            for (File file : files) {
                deleteDir(file);
            }
        }

        return dirFile.delete();
    }

    /**
     * 获取conf文件夹所在路径
     *
     * @return 路径地址
     */
    private String getConfPath() {
        String classpath = this.getClass().getResource("/").getPath();
        String confPath = classpath + "../conf/";
        if (new File(confPath).exists()) {
            return confPath;
        } else {
            return classpath;
        }
    }

    public void destroy() {
        executor.shutdownNow();
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    /**
     * 配置对应对象
     */
    public static class ConfigItem {

        private Long   id;
        private String category;
        private String name;
        private String content;
        private long   modifiedTime;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getCategory() {
            return category;
        }

        public void setCategory(String category) {
            this.category = category;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getContent() {
            return content;
        }

        public void setContent(String content) {
            this.content = content;
        }

        public long getModifiedTime() {
            return modifiedTime;
        }

        public void setModifiedTime(long modifiedTime) {
            this.modifiedTime = modifiedTime;
        }
    }
}
