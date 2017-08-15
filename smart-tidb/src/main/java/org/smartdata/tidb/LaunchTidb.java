package org.smartdata.tidb;

/**
 * Created by philo on 17-8-11.
 */
public class LaunchTidb {
    public static void main(String []args){
        TidbServer tidbServer=new TidbServer();
        String tidb_args= new String("--P=4000 --status=10080");
        tidbServer.start(tidb_args);
    }
}
