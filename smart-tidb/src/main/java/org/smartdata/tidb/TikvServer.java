package org.smartdata.tidb;

/**
 * Created by philo on 17-8-11.
 */
import com.sun.jna.Library;
import com.sun.jna.Native;

public class TikvServer {
    public interface Tikv extends Library {
        void call_kvserver(String cmd_args);
    }
    public void start(String args){
        Tikv kv= Native.loadLibrary("./libtikv.so",Tikv.class);
        StringBuffer strbuffer=new StringBuffer();
        strbuffer.append("TiKV");  //@ App::new("TiKV") in start.rs, "TiKV" is the flag name used for parsing
        strbuffer.append(" ");
        strbuffer.append(args);

        System.out.println("starting TiKV..");
        kv.call_kvserver(strbuffer.toString());
    }

}
