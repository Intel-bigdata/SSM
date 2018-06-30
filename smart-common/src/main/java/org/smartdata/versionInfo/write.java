package org.smartdata.versionInfo;
import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

public class write  {


    File directory = new File("");
    String pom = directory.getAbsolutePath() + "/" + "pom.xml";
    String pwd = directory.getAbsolutePath();

    public void execute() {
        try {

            File f = new File(directory.getAbsolutePath() + "/" + "common-version-info.properties");
            FileWriter out = new FileWriter(f);
            out.write("version=" + getVersionInfo(pom) + "\n");
            out.write("revision="+getSCMCommit()+"\n");
            out.write("user="+getSCMUser()+"\n");
            out.write("date=" + getBuildTime() + "\n");
            out.write("url="+getSCMUri()+"\n");
            out.flush();
            out.close();


        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private  String getBuildTime () {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return dateFormat.format(new Date());
    }



    private String getVersionInfo (String pom) throws IOException {

        File fl = new File(pom);
        FileReader fr = new FileReader(fl);
        String st = null;
        BufferedReader br = new BufferedReader(fr);
        while ((st = br.readLine()) != null) {
            if (st.contains("<version>")) {
                return st.trim().substring("<version>".length(), st.trim().length() - "</version>".length());
            }
        }
        return "Not found";
    }


    private String getSCMUri (){
        List<String> scm=execCmd("git remote -v");
        String uri = "Unknown";
        for (String s : scm) {
            if (s.startsWith("origin") && s.endsWith("(fetch)")) {
                uri = s.substring("origin".length());
                uri = uri.substring(0, uri.length() - "(fetch)".length());

                break;
            }


        }
        return uri.trim();
    }


    private String getSCMCommit (){
        List<String> scm=execCmd("git log -n 1");
        String commit = "Unknown";
        for (String s : scm) {
            if (s.startsWith("commit")) {
                commit = s.substring("commit".length());
                break;
            }

        }
        return commit.trim();
    }



    private String getSCMUser (){
        List<String> scm=execCmd("whoami");
        String user = "Unknown";
        for (String s : scm) {
                user=s.trim();
                break;
            }

        return user;
    }


    public List<String> execCmd (String cmd){
        String command=new String(cmd);
        command= "/bin/sh -c "+command;
        List<String> list = new ArrayList<String>();
        try {
            Runtime rt = Runtime.getRuntime();
            //执行命令, 最后一个参数，可以使用new File("path")指定运行的命令的位置
            Process proc = rt.exec(cmd, null, null);
            InputStream stderr = proc.getInputStream();
            InputStreamReader isr = new InputStreamReader(stderr, "GBK");
            BufferedReader br = new BufferedReader(isr);
            String line = "";
            while ((line = br.readLine()) != null) { // 打印出命令执行的结果
                list.add(line);

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }
    public static void main (String[] args) {

        write w=new write();
        w.execute();

        }
}
