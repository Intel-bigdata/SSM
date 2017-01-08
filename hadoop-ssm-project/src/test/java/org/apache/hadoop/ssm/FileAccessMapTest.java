package org.apache.hadoop.ssm;
import org.apache.hadoop.hdfs.protocol.FilesAccessInfo;
import org.apache.hadoop.hdfs.protocol.NNEvent;
import org.junit.Test;
import org.apache.hadoop.ssm.api.Expression.*;
import org.apache.hadoop.ssm.parse.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static junit.framework.TestCase.assertEquals;
/**
 * Created by root on 11/22/16.
 */
public class FileAccessMapTest {
  @Test
  public void testGetMap() {
    FileAccessMap fam = new FileAccessMap();
    Integer cot = new Integer(1);
    FileAccess faa = new FileAccess("/A/a.txt", cot);
    FileAccess fab = new FileAccess("/A/b.txt", cot);
    fam.put(faa.getFileName(), faa);
    fam.put(fab.getFileName(), fab);
    HashMap<String, FileAccess> hm=fam.getMap();
    int count = 0;
    boolean flag = true;
    for (String key : hm.keySet()) {
      count++;
      if (hm.get(key).getAccessCount().compareTo(1) != 0) {
        flag = false;
      }
    }
    if (count != 2) {
      flag = false;
    }
//To determine whether a file number and access number
    assertEquals(true, flag);
  }
  @Test
  public void testPut() {
    Integer cot = new Integer(0);
    FileAccess fa = new FileAccess("/A/a.txt", cot);
    FileAccessMap fam = new FileAccessMap();
    fam.put(fa.getFileName(), fa);
    assertEquals(true, fam.containsKey(fa.getFileName()));
  }
  @Test
  public void testGet() {
    Integer cot = new Integer(0);
    FileAccess fa = new FileAccess("/A/a.txt", cot);
    FileAccessMap fam = new FileAccessMap();
    fam.put(fa.getFileName(), fa);
    assertEquals(fa.getFileName(), fam.get("/A/a.txt").getFileName());
  }
  @Test
  public void testRemove() {
    Integer cot = new Integer(0);
    FileAccess fa = new FileAccess("/A/a.txt", cot);
    FileAccessMap fam = new FileAccessMap();
    fam.put(fa.getFileName(), fa);
    fam.remove(fa.getFileName());
    assertEquals(false, fam.containsKey(fa.getFileName()));
  }
  @Test
  public void testContainsKey() {
    Integer cot = new Integer(0);
    FileAccess fa = new FileAccess("/A/a.txt", cot);
    FileAccessMap fam = new FileAccessMap();
    fam.put(fa.getFileName(), fa);
    assertEquals(true, fam.containsKey("/A/a.txt"));
  }
  @Test
  public void testRenameFileNoRule() {
    Integer cot = new Integer(4);
    Integer cot1 = new Integer(6);
    FileAccess fa = new FileAccess("/A/a.txt", cot);
    FileAccess fa1 = new FileAccess("/A/a1.txt", cot);
    FileAccess fb = new FileAccess("/B/b.txt", cot1);
    FileAccessMap fam = new FileAccessMap();
    fam.put(fa.getFileName(), fa);
    fam.put(fa1.getFileName(), fa1);
    fam.put(fb.getFileName(), fb);
    String srcName = fa.getFileName();
    String dstName = "/C/c.txt";
    fam.rename(srcName, dstName, null);
    assertEquals(true, fam.containsKey(dstName));
    assertEquals(false, fam.containsKey(srcName));
//count copy or not
    assertEquals(0, fam.get(dstName).getAccessCount().compareTo(4));
//count merge or not
    srcName = dstName;
    dstName = "/B/b.txt";
    fam.rename(srcName, dstName, null);
    assertEquals(0, fam.get(dstName).getAccessCount().compareTo(10));
  }
  @Test
  public void testRenameDirNoRule() {
    Integer cot = new Integer(1);
    Integer cot1 = new Integer(2);
    FileAccess fa = new FileAccess("/A/a/1.txt", cot);
    FileAccess fa1 = new FileAccess("/A/a/2.txt", cot);
    FileAccess fb = new FileAccess("/B/b/1.txt", cot1);
    FileAccessMap fam = new FileAccessMap();
    fam.put(fa.getFileName(), fa);
    fam.put(fa1.getFileName(), fa1);
    fam.put(fb.getFileName(), fb);
    String srcName;
    String dstName;
//rename second-level directory
    srcName = "/A/a";
    dstName = "/A/b";
    fam.rename(srcName, dstName, null);
    assertEquals(false, fam.containsKey("/A/a/1.txt"));
    assertEquals(false, fam.containsKey("/A/a/2.txt"));
    assertEquals(true, fam.containsKey("/A/b/1.txt"));
    assertEquals(true, fam.containsKey("/A/b/2.txt"));
////rename first-level directory
    srcName = "/A/";
    dstName = "/B/";
    fam.rename(srcName, dstName, null);
    assertEquals(false, fam.containsKey("/A/b/1.txt"));
    assertEquals(false, fam.containsKey("/A/b/2.txt"));
    assertEquals(true, fam.containsKey("/B/b/1.txt"));
    assertEquals(true, fam.containsKey("/B/b/2.txt"));
//count merge or not when rename directory
    dstName = "/B/b/1.txt";
    assertEquals(0, fam.get(dstName).getAccessCount().compareTo(3));
  }
  @Test
  public void testRenameWtRule() {
    Integer cot = new Integer(1);
    FileAccess fa = new FileAccess("/A/a.txt", cot);
    FileAccess fa1 = new FileAccess("/A/a1.txt", cot);
    FileAccess fb = new FileAccess("/A/b1.txt", cot);
    FileAccess fb1 = new FileAccess("/A/b1.txt", cot);
    FileAccessMap fam = new FileAccessMap();
    fam.put(fa.getFileName(), fa);
    fam.put(fa1.getFileName(), fa1);
    fam.put(fb.getFileName(), fb);
    fam.put(fb1.getFileName(), fb1);
    SSMRule ruleObject = SSMRuleParser.parseAll("file.path matches('/A/[a-z]*') : accessCount (10 min) >= 50 | ssd").get();
    FileFilterRule fileFilterRule = ruleObject.fileFilterRule();
    fam.rename(fa.getFileName(), "/A/1.txt", fileFilterRule);
    fam.rename(fa1.getFileName(), "a.txt", fileFilterRule);
    fam.rename(fb.getFileName(), "/B/b.txt", fileFilterRule);
    fam.rename(fb1.getFileName(), "/A/B/b1.txt", fileFilterRule);
    assertEquals(false, fam.containsKey("/A/1.txt"));
    assertEquals(false, fam.containsKey("a.txt"));
    assertEquals(false, fam.containsKey("/B/b.txt"));
    assertEquals(false, fam.containsKey("/A/B/b1.txt"));
    assertEquals(false, fam.containsKey(fa.getFileName()));
    assertEquals(false, fam.containsKey(fa1.getFileName()));
    assertEquals(false, fam.containsKey(fb.getFileName()));
    assertEquals(false, fam.containsKey(fb1.getFileName()));
  }
  @Test
  public void deleteTest() {
    Integer cot = new Integer(1);
    String nameAa = "/A/a.txt";
    String nameAa1 = "/A/a1.txt";
    String nameBb = "/B/b.txt";
    String nameBb1 = "/B/b1.txt";
    FileAccess fa = new FileAccess(nameAa, cot);
    FileAccess fa1 = new FileAccess(nameAa1, cot);
    FileAccess fb = new FileAccess(nameBb, cot);
    FileAccess fb1 = new FileAccess(nameBb1, cot);
    FileAccessMap fam = new FileAccessMap();
    fam.put(fa.getFileName(), fa);
    fam.put(fa1.getFileName(), fa1);
    fam.put(fb.getFileName(), fb);
    fam.put(fb1.getFileName(), fb1);
    fam.delete(nameBb);
    assertEquals(false, fam.containsKey(nameBb));
    assertEquals(true, fam.containsKey(nameBb1));
    fam.delete("/A/");
    assertEquals(false, fam.containsKey(nameAa));
    assertEquals(false, fam.containsKey(nameAa1));
  }
  @Test
  public void testPNENoRule() {
    Integer cot = new Integer(1);
    FileAccess fa = new FileAccess("/A/a.txt", cot);
    FileAccess fa1 = new FileAccess("/A/a1.txt", cot);
    FileAccess fb = new FileAccess("/B/b.txt", cot);
    FileAccess fb1 = new FileAccess("/B/b1.txt", cot);
    FileAccessMap fam = new FileAccessMap();
    fam.put(fa.getFileName(), fa);
    fam.put(fa1.getFileName(), fa);
    fam.put(fb.getFileName(), fa);
    fam.put(fb1.getFileName(), fb1);
    NNEvent nnEvent = new NNEvent(0);
    NNEvent nnEvent1 = new NNEvent(1, fa1.getFileName());
    NNEvent nnEvent2 = new NNEvent(2, fb.getFileName(), "/A/b.txt");
    NNEvent nnEvent3 = new NNEvent(2, fb1.getFileName(), "/A/a3.txt");
    List<NNEvent> nnEventList = new ArrayList<NNEvent>();
    nnEventList.add(nnEvent);
    nnEventList.add(nnEvent1);
    nnEventList.add(nnEvent2);
    nnEventList.add(nnEvent3);
    FilesAccessInfo fai = new FilesAccessInfo();
    fai.setNnEvents(nnEventList);
    fam.processNnEvents(fai);
    assertEquals(true, fam.containsKey(fa.getFileName()));
    assertEquals(false, fam.containsKey(fa1.getFileName()));
    assertEquals(true, fam.containsKey("/A/b.txt"));
    assertEquals(true, fam.containsKey("/A/a3.txt"));
  }
  @Test
  public void testPcesNnEventsWtRule() {
    Integer cot = new Integer(1);
    String strFa = "/A/a";
    String strFaa = "/A/aa";
    String strFb = "/A/b";
    String strFbb = "/A/bb";
    FileAccess fa = new FileAccess(strFa, cot);
    FileAccess faa = new FileAccess(strFaa, cot);
    FileAccess fb = new FileAccess(strFb, cot);
    FileAccess fbb = new FileAccess(strFbb, cot);
    FileAccessMap fam = new FileAccessMap();
    fam.put(strFa, fa);
    fam.put(strFaa, faa);
    fam.put(strFb, fb);
    fam.put(strFbb, fbb);
    NNEvent nnEvent = new NNEvent(2, strFa, "/A/1.txt");
    NNEvent nnEvent1 = new NNEvent(2, strFaa, "a.txt");
    NNEvent nnEvent2 = new NNEvent(2, strFb, "/B/b.txt");
    NNEvent nnEvent3 = new NNEvent(2, strFbb, "/A/B/b1.txt");
    List<NNEvent> nnEventList = new ArrayList<NNEvent>();
    nnEventList.add(nnEvent);
    nnEventList.add(nnEvent1);
    nnEventList.add(nnEvent2);
    nnEventList.add(nnEvent3);
    FilesAccessInfo fai = new FilesAccessInfo();
    fai.setNnEvents(nnEventList);
    SSMRule ruleObject = SSMRuleParser.parseAll("file.path matches('/A/[a-z]*') : accessCount (10 min) >= 50 | ssd").get();
    FileFilterRule fileFilterRule = ruleObject.fileFilterRule();
    fam.processNnEvents(fai, fileFilterRule);
    assertEquals(false, fam.containsKey("/A/1.txt"));
    assertEquals(false, fam.containsKey("a.txt"));
    assertEquals(false, fam.containsKey("/B/b.txt"));
    assertEquals(false, fam.containsKey("/A/B/b1.txt"));
    assertEquals(false, fam.containsKey("/A/a.txt"));
    assertEquals(false, fam.containsKey("/A/a1.txt"));
    assertEquals(false, fam.containsKey("/A/b.txt"));
    assertEquals(false, fam.containsKey("/A/b1.txt"));
  }
  //the first constructor of the AccessCounter
  @Test
  public void testUpdateMapNoRuleOne() {
    Integer cotFa = new Integer(1);
    String strFa = "/A/a.txt";
    FileAccess fa = new FileAccess(strFa, cotFa);
    FileAccessMap fam = new FileAccessMap();
    fam.put(strFa, fa);
    Map<String, Integer> map = new HashMap<String, Integer>();
    map.put("/B/a.txt", 1);
    map.put("/A/a.txt", 1);
    FilesAccessInfo fai = new FilesAccessInfo();
    fai.setAccessCounter(map);
    fam.updateFileMap(fai);
    assertEquals(true, fam.containsKey("/B/a.txt"));
    assertEquals(0, fam.get("/B/a.txt").getAccessCount().compareTo(1));
    assertEquals(true, fam.containsKey("/A/a.txt"));
    assertEquals(0, fam.get("/A/a.txt").getAccessCount().compareTo(2));
  }
  //the second constructor of the AccessCounter
  @Test
  public void testUpdateMapNoRuleTwo() {
    FileAccessMap fam = new FileAccessMap();
    Integer cotFa = new Integer(1);
    String strFa = "/A/a.txt";
    FileAccess fa = new FileAccess(strFa, cotFa);
    fam.put(strFa, fa);
    List<String> fileList = new ArrayList<String>();
    List<Integer> countList = new ArrayList<Integer>();
    fileList.add("/A/a.txt");
    fileList.add("/B/a.txt");
    countList.add(1);
    countList.add(1);
    FilesAccessInfo fai = new FilesAccessInfo();
    fai.setAccessCounter(fileList, countList);
    fam.updateFileMap(fai);
    assertEquals(0, fam.get("/B/a.txt").getAccessCount().compareTo(1));
    assertEquals(0, fam.get("/A/a.txt").getAccessCount().compareTo(2));
  }
  @Test
  public void testUpdateMapWiRule() {
//create the data
    Integer cotFa = new Integer(1);
    String strFa = "/A/a";
    FileAccess fa = new FileAccess(strFa, cotFa);
    FileAccessMap fam = new FileAccessMap();
    fam.put(strFa, fa);
    HashMap<String, Integer> map = new HashMap<String, Integer>();
    map.put(strFa, 1);
    map.put("/A/b", 1);
    map.put("/A/a1", 1);
    map.put("/B/a", 1);
    map.put("/A/1", 1);
    map.put("a", 1);
    map.put("/A/B/bb", 1);
    map.put("@/a", 1);
    FilesAccessInfo fai = new FilesAccessInfo();
    fai.setAccessCounter(map);
    SSMRule ruleObject = SSMRuleParser.parseAll("file.path matches('/A/[a-z]*') : accessCount (10 min) >= 50 | ssd").get();
    FileFilterRule fileFilterRule = ruleObject.fileFilterRule();
    fam.updateFileMap(fai, fileFilterRule);
    assertEquals(0, fam.get(strFa).getAccessCount().compareTo(2));
    assertEquals(true, fam.containsKey(strFa));
    assertEquals(true, fam.containsKey("/A/b"));
    assertEquals(false, fam.containsKey("/A/a1"));
    assertEquals(false, fam.containsKey("/B/a"));
    assertEquals(false, fam.containsKey("/A/1"));
    assertEquals(false, fam.containsKey("a"));
    assertEquals(false, fam.containsKey("/A/B/b"));
    assertEquals(false, fam.containsKey("@/a"));
  }
}
