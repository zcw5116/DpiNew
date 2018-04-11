package com.zyuc.dpi.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by zhoucw on 上午9:24.
 */
public class HDFSFileUtils {

    /**
     * demo:
     * String user = "epciot/chinatelecom@CHINATELECOM.CN";
     * String keytabPath = "/slview/nms/cfg/epciot.keytab";
     * Configuration conf = getConfig(user, keytabPath);
     *
     * @param user
     * @param keytabPath
     * @return
     */
    public static Configuration getConfig(String user, String keytabPath) {
        Configuration conf = new Configuration();
        conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));

        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());

        System.setProperty("java.security.krb5.conf", "/slview/nms/cfg/krb5.conf");

        conf.set("hadoop.security.authentication", "kerberos");
        //conf.set("java.security.krb5.kdc", "/home/username/kdc.conf");
        conf.set("java.security.krb5.realm", "CHINATELECOM.CN");
        //conf.set("mapreduce.job.queuename", "queue_username");//设置资源队列名
        UserGroupInformation.setConfiguration(conf);
        try {
            UserGroupInformation.loginUserFromKeytab(user, keytabPath);
        } catch (Exception e) {
            System.out.println("身份认证error： " + e.getMessage());
            e.printStackTrace();
        }
        return conf;
    }

    public static Configuration getConfig() {
        Configuration conf = new Configuration();
        conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
        return conf;
    }

    /**
     * @param fileSystem
     * @param dir
     */
    public static void ensureHDFSDirExists(FileSystem fileSystem, String dir) {
        try {
            if (!fileSystem.exists(new Path(dir))) {
                fileSystem.mkdirs(new Path(dir));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void moveLocalToHDFS(final FileSystem fileSystem, String localPath,
                                       final String hdfsParentDir, final String loadTime,
                                       String fileWildCard, int threadNum) {
        boolean isLocalMoved = true;
        localToHDFS(fileSystem, localPath, hdfsParentDir, loadTime, fileWildCard, threadNum, isLocalMoved);
    }


    public static void copyLocalToHDFS(final FileSystem fileSystem, String localPath,
                                       final String hdfsParentDir, final String loadTime,
                                       String fileWildCard, int threadNum) {
        try {
            File dir = new File(localPath);
            File[] files = dir.listFiles(new FileFilter(fileWildCard));

            Set<Path> fileLocals = new HashSet<Path>();
            for (File file : files) {
                fileLocals.add(new Path(file.getAbsolutePath()));
            }
            ExecutorService executor = Executors.newFixedThreadPool(threadNum);
            for (final Path fileLocal : fileLocals) {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {

                            String targetTimeStr = new SimpleDateFormat("yyyyMMddHHmm").format(new Date());
                            if (!"-1".equals(loadTime)) {
                                targetTimeStr = loadTime;
                            }

                            String destDir = hdfsParentDir + "/" + targetTimeStr;
                            ensureHDFSDirExists(fileSystem, destDir);
                            fileSystem.copyFromLocalFile(fileLocal, new Path(destDir));

                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
            }

            executor.shutdown();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     *
     */

    public static void renameFile(final FileSystem fileSystem, String localPath,
                                  final String hdfsParentDir, final String loadTime,
                                  String fileWildCard){

        File dir = new File(localPath);
        File[] files = dir.listFiles(new FileFilter(fileWildCard));

        for (File file : files) {
            String fileWorking = file.getAbsolutePath();
            String fileLocal = fileWorking.substring(0, fileWorking.lastIndexOf("."));
           file.renameTo(new File(fileLocal));
        }
    }

    /**
     * @param fileSystem
     * @param localPath
     * @param hdfsParentDir
     * @param loadTime
     * @param fileWildCard
     */
    public static void localToHDFS(final FileSystem fileSystem, String localPath,
                                   final String hdfsParentDir, final String loadTime,
                                   String fileWildCard, int threadNum, final boolean isLocalMoved) {


        try {
            File dir = new File(localPath);
            File[] files = dir.listFiles(new FileFilter(fileWildCard));

            Set<Path> fileLocalWorkings = new HashSet<Path>();
            for (File file : files) {
                String fileWorking = file.getAbsolutePath() + ".working";
                if (file.renameTo(new File(fileWorking))) {
                    fileLocalWorkings.add(new Path(fileWorking));
                }

            }

            ExecutorService executor = Executors.newFixedThreadPool(threadNum);

            for (final Path fileLocalWorking : fileLocalWorkings) {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {

                            String targetTimeStr = new SimpleDateFormat("yyyyMMddHHmm").format(new Date());
                            if (!"-1".equals(loadTime)) {
                                targetTimeStr = loadTime;
                            }

                            String destDir = hdfsParentDir + "/" + targetTimeStr;
                            ensureHDFSDirExists(fileSystem, destDir);
                            if (isLocalMoved) {
                                fileSystem.moveFromLocalFile(fileLocalWorking, new Path(destDir));
                            } else {
                                fileSystem.copyFromLocalFile(fileLocalWorking, new Path(destDir));
                            }

                            String fileWorkingName = fileLocalWorking.getName();
                            fileSystem.rename(new Path(destDir + "/" + fileWorkingName), new Path(destDir + "/" + fileWorkingName.substring(0, fileWorkingName.lastIndexOf("."))));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
            }

            executor.shutdown();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    // iot/data/src/{201709081234}
    public static void main(String[] args) throws IOException, URISyntaxException {

        Configuration conf = getConfig();
        FileSystem fileSystem = FileSystem.get(conf);
        String localPath = args[0];  // "/hadoop/bd/b1"
        String hdfsParentDir = args[1]; // "/tmp"
        String loadTime = args[2]; //"201805";
        String wild = args[3]; // "^p.*[0-9]$";
        int threadNum = Integer.parseInt(args[4]);

      copyLocalToHDFS(fileSystem, localPath, hdfsParentDir, loadTime, wild, threadNum);
       // renameFile(fileSystem, localPath, hdfsParentDir, loadTime, wild);

    }


}
