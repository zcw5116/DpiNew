package com.zyuc.dpi.hdfs;

import java.util.*;

/**
 * Created by zhoucw on 18-5-20 下午6:13.
 */
public class HdfsFile {
    String name;
    Long size;

    public HdfsFile(String name, Long size) {
        this.name = name;
        this.size = size;
    }

    public String getName(){
        return this.name;
    }

    public long getSize(){
        return this.size;
    }
    public String toString() {
        return "[" + this.name + ", " + this.size + "]";
    }


    public static void main(String[] args) throws InterruptedException {

        // 如果使用算法2， 排序为asc。 如果使用算法3，排序为desc
        TreeSet<HdfsFile> treeSet = new TreeSet<HdfsFile>(new HdfsFileSizeComparator("desc"));
        treeSet.add(new HdfsFile("tet90-5", 50l));
        treeSet.add(new HdfsFile("tet90", 90l));
        treeSet.add(new HdfsFile("tet80", 80l));
        treeSet.add(new HdfsFile("tet79", 79l));
        treeSet.add(new HdfsFile("tet50", 50l));
        treeSet.add(new HdfsFile("tet38", 38l));
        treeSet.add(new HdfsFile("tet30", 30l));
        treeSet.add(new HdfsFile("tet20", 20l));
        treeSet.add(new HdfsFile("tet10", 10l));

        System.out.println(treeSet);

         //m1(treeSet);
        //m2(treeSet);
        m3(treeSet);


    }

    public static void m1(TreeSet<HdfsFile> treeSet) {
        List<String> fileGroups = new ArrayList<String>();

        int num = 0;

        HdfsFile first = treeSet.pollFirst();
        Long size = first.size;
        fileGroups.add(first.name);

        while (treeSet.size() > 0) {
            HdfsFile second = treeSet.first();
            if(size + second.size < 128){
                fileGroups.set(num, fileGroups.get(num) + "#" + second.name);
                size = size + second.size;
                first = treeSet.pollFirst();
            }else{
                num = num + 1;
                first = treeSet.pollFirst();
                size = first.size;
                fileGroups.add(first.name);
            }
        }

        for (int i = 0; i < fileGroups.size(); i++) {
            if(fileGroups.get(i) != null){
                System.out.println(fileGroups.get(i));
            }
        }
    }


    // 实现类似装箱算法
    public static void m2(TreeSet<HdfsFile> treeSet) {

        TreeSet<HdfsFile> newTreeSet = new TreeSet<HdfsFile>(new HdfsFileSizeComparator("asc"));
        List<String> fileGroups = new ArrayList<String>();


        int num = 0;

        HdfsFile first = treeSet.first();
        fileGroups.add(first.name);

        while (treeSet.size() > 0) {
            HdfsFile last = treeSet.last();
            if(last == first){// 集合中只有一个元素
                newTreeSet.add(treeSet.pollLast());
            }
            else if(first.size + last.size < 128){
                long size = first.size + last.size;
                System.out.println("size:" + size);
                treeSet.pollFirst();
                treeSet.pollLast();
                treeSet.add(new HdfsFile(first.getName() + "#" + last.getName(), first.size + last.size));
                first = treeSet.first();

            }else{ // 最后一个元素不能再加入新的文件
                newTreeSet.add(treeSet.pollLast());
            }
        }

        System.out.println("newTreeSet:" + newTreeSet);
    }


    /**
     * 将hdfsfile和newTreeSet元素比较, 从newTreeSet中最大的元素比较， 如果两个元素的size之和小于128， 那么就可以合并。
     * 如果之和大于等于128或者newTreeSet的元素为空， 则将hdfsfile添加为新元素。
     * @param newTreeSet
     * @param hdfsfile
     */
   public static void setGroup(TreeSet<HdfsFile> newTreeSet, HdfsFile hdfsfile, long fileSize){
        Iterator<HdfsFile> files = newTreeSet.iterator();
        int flag = 0;
        HdfsFile targetFile = null;
        while(files.hasNext()){
            HdfsFile file = files.next();
            if(hdfsfile.size + file.size < fileSize){
                targetFile = file;
                break;
            }
        }
        if(targetFile != null){
            newTreeSet.remove(targetFile);
            newTreeSet.add(new HdfsFile(hdfsfile.getName() + "#" + targetFile.getName(), hdfsfile.getSize() + targetFile.getSize()));

        }else{
            newTreeSet.add(hdfsfile);
        }
    }

    // 实现类似装箱算法
    public static void m3(TreeSet<HdfsFile> treeSet) {

        TreeSet<HdfsFile> newTreeSet = new TreeSet<HdfsFile>(new HdfsFileSizeComparator("desc"));

        Iterator<HdfsFile> iterator = treeSet.iterator();

        while(iterator.hasNext()) {
            setGroup(newTreeSet, iterator.next(), 128*1024*1024);
        }

        System.out.println(newTreeSet);


    }

}

