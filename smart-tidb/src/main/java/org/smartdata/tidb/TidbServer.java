package org.smartdata.tidb;

/**
 * Created by philo on 17-8-11.
 */

import com.sun.jna.*;

public class TidbServer{
    //define an interface and declare the func with the same name as that exported by main.go
    public interface Tidb extends Library{
        void callMain(String cmdArgs);
    }

    public void start(String args){

        Tidb tidb= Native.loadLibrary("./libtidb.so",Tidb.class);

        System.out.println("starting TiDB..");
        tidb.callMain(args);

    }

}