package org.smartdata.versionInfo;
import java.io.IOException;

import static java.lang.Thread.sleep;

public class main_test {

    public static void main (String[] args)  {

        write w=new write();
        w.execute();
        try {
            sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        read t=new read();
        try {
            t.pirntInfo("common-version-info.properties");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}


