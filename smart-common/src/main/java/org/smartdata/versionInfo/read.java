package org.smartdata.versionInfo;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class read {


    public String getVersion(FileReader in) throws IOException {
        String st = null;
        BufferedReader br=new BufferedReader(in);
        while ((st = br.readLine()) != null) {
            if (st.contains("version")) {
                return st.substring("version=".length());
            }
        }
        return "Not found";
    }

    public String getUrl(FileReader in) throws IOException {
        String st = null;
        BufferedReader br=new BufferedReader(in);
        while ((st = br.readLine()) != null) {
            if (st.contains("url")) {
                return st.substring("url=".length());
            }
        }
        return "Not found";
    }

    public String getRevision(FileReader in) throws IOException {
        String st = null;
        BufferedReader br=new BufferedReader(in);
        while ((st = br.readLine()) != null) {
            if (st.contains("revision")) {
                return st.substring("revision=".length());
            }
        }
        return "Not found";
    }
    public String getTime(FileReader in) throws IOException {
        String st = null;
        BufferedReader br=new BufferedReader(in);
        while ((st = br.readLine()) != null) {
            if (st.contains("date")) {
                return st.substring("date=".length());
            }
        }
        return "Not found";
    }

    public void pirntInfo(String versionInfoFile ) throws IOException {
        File directory = new File("");
        String s=directory.getAbsolutePath()+"/"+ versionInfoFile;

        File f=new File(s);
        //  FileWriter out=new FileWriter(f);
        FileReader v=new FileReader(f);
        FileReader u=new FileReader(f);
        FileReader r=new FileReader(f);
        FileReader d=new FileReader(f);

        String version=this.getVersion(v);
        String url=this.getUrl(u);
        String revision=this.getRevision(r);
        String time=this.getTime(d);

        System.out.println("SSM " + version);
        System.out.println("Subversion " + url + " -r " + revision);
        System.out.println("Compiled"  + " on " + time);

        v.close();
        u.close();
        r.close();
        d.close();

    }

}
