package com.zyuc.dpi.hdfs;

import java.util.Comparator;

/**
 * Created by zhoucw on 18-5-22 上午10:16.
 */
public class HdfsFileSizeComparator  implements Comparator<HdfsFile> {
    private String order;

    public HdfsFileSizeComparator(String order){
        this.order = order;
    }
    @Override
    public int compare(HdfsFile f1, HdfsFile f2) {
        Long m = f1.size - f2.size;
        int r = 0;
        if(m>0l){
            r = 1;
        }else if(m<0l){
            r = -1;
        }else{
            r = f1.name.compareTo(f2.name);
        }
        if(this.order == "desc"){
          r = -r;
        }
        return r;
    }
}
