package com.zyuc.dpi.utils;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;
import java.sql.Connection;

/**
 * Created by zhoucw on 下午2:48.
 */
public class ConfigUtil {

    private static Config config;

    public static Config getConfig(){
        config = ConfigFactory.load();
        return config;
    }

    public static Config getConfig(String configPath) {
        config = ConfigFactory.parseFile(new File(configPath));
        return config;
    }
}
