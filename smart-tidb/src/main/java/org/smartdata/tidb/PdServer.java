package org.smartdata.tidb;

import com.sun.jna.Library;
import com.sun.jna.Native;

/**
 * Created by philo on 17-8-11.
 */
public class PdServer {
    public interface Pd extends Library {
        void callMain(String cmdArgs);
    }
    public void start(String args){
        Pd pd= Native.loadLibrary("./libpd.so",Pd.class);

        System.out.println("starting PD..");
        pd.callMain(args);
    }

}
