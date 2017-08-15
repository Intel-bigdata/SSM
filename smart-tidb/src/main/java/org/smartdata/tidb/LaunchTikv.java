package org.smartdata.tidb;

/**
 * Created by philo on 17-8-11.
 */
public class LaunchTikv {
    public static void main(String []args){
        TikvServer tikvServer=new TikvServer();
        String tikv_args=new String("--pd=127.0.0.1:2379 --data-dir=tikv"); //--log-file=tikv.log");
        tikvServer.start(tikv_args);
    }
}
