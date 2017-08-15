package org.smartdata.tidb;

/**
 * Created by philo on 17-8-11.
 */
public class LaunchPd {

    public static void main(String []args) {
        PdServer pdServer = new PdServer();
        String pd_args = new String("--data-dir=pd --log-file=pd.log");
        pdServer.start(pd_args);
    }

}
