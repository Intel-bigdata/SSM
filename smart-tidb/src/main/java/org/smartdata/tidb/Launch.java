package org.smartdata.tidb;

/**
 * Created by philo on 17-8-11.
 */
public class Launch {
    public static void main(String []args){
        PdServer pdServer=new PdServer();
        TikvServer tikvServer=new TikvServer();
        TidbServer tidbServer=new TidbServer();

        String pd_args=new String("--data-dir=pd --log-file=pd.log");
        String tikv_args=new String("--pd=127.0.0.1:2379 --data-dir=tikvdata --log-file=tikv.log");
        String tidb_args= new String("-store tikv  -path 127.0.0.1:2379");

        pdServer.start(pd_args);
        tikvServer.start(tikv_args);
        tidbServer.start(tidb_args);

    }
}
