package com.zyuc.dpi.hdfs;

import java.io.File;
import java.io.FilenameFilter;

/**
 * 根据正则表达式过滤磁盘上的文件
 * Created by zhoucw on 下午2:50.
 */
public class FileFilter implements FilenameFilter {
    private String filterReg;

    public FileFilter(String filterReg) {
        this.filterReg = filterReg;
    }

    @Override
    public boolean accept(File dir, String name) {

        return name.matches(filterReg);
    }

    public static void main(String[] args) {
        File dir = new File("/hadoop/bd/b1");
        File[] files = dir.listFiles(new FileFilter("^p.*[0-9]$"));
        for(File file:files){
            System.out.println(file);
        }
    }
}
