package org.smartdata.versioninfo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class VersionInfoRead {


  public String getVersion(FileReader in) throws IOException {
    String st = null;
    BufferedReader br = new BufferedReader(in);
    while ((st = br.readLine()) != null) {
      if (st.contains("version")) {
        return st.substring("version=".length());
      }
    }
    return "Not found";
  }

  public String getUrl(FileReader in) throws IOException {
    String st = null;
    BufferedReader br = new BufferedReader(in);
    while ((st = br.readLine()) != null) {
      if (st.contains("url")) {
        return st.substring("url=".length());
      }
    }
    return "Not found";
  }

  public String getRevision(FileReader in) throws IOException {
    String st = null;
    BufferedReader br = new BufferedReader(in);
    while ((st = br.readLine()) != null) {
      if (st.contains("revision")) {
        return st.substring("revision=".length());
      }
    }
    return "Not found";
  }

  public String getTime(FileReader in) throws IOException {
    String st = null;
    BufferedReader br = new BufferedReader(in);
    while ((st = br.readLine()) != null) {
      if (st.contains("date")) {
        return st.substring("date=".length());
      }
    }
    return "Not found";
  }

  public String getUser(FileReader in) throws IOException {
    String st = null;
    BufferedReader br = new BufferedReader(in);
    while ((st = br.readLine()) != null) {
      if (st.contains("user")) {
        return st.substring("user=".length());
      }
    }
    return "Not found";
  }


  public void pirntInfo(String versionInfoFile) throws IOException {
    File directory = new File("");
    String s = directory.getAbsolutePath() + "/" + versionInfoFile;

    File f = new File(s);
    //  FileWriter out=new FileWriter(f);
    FileReader v = new FileReader(f);
    FileReader u = new FileReader(f);
    FileReader r = new FileReader(f);
    FileReader d = new FileReader(f);
    FileReader k = new FileReader(f);

    String version = this.getVersion(v);
    String url = this.getUrl(u);
    String revision = this.getRevision(r);
    String time = this.getTime(d);
    String user = this.getUser(k);

    System.out.println("SSM " + version);
    System.out.println("Subversion " + url + " -r " + revision);
    System.out.println("Compiled by " + user + " on " + time);

    v.close();
    u.close();
    r.close();
    d.close();
    k.close();
  }

  public static void main(String[] args) {

    VersionInfoRead t = new VersionInfoRead();
    try {
      t.pirntInfo("common-version-info.properties");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
