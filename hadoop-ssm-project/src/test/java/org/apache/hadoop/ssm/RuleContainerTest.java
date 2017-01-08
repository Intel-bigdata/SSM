package org.apache.hadoop.ssm;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.FilesAccessInfo;
import org.apache.hadoop.hdfs.protocol.NNEvent;
import org.apache.hadoop.ssm.api.Expression.*;
import org.apache.hadoop.ssm.parse.SSMRuleParser;
import org.junit.Test;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static junit.framework.TestCase.assertEquals;
/**
 * Created by root on 11/10/16.
 */
public class RuleContainerTest {
    /**
     * @param
     * @C not confirm Account_rule
     * @D delete
     * @E rename conform to the name_rules
     * @F rename not conform to the name_rules
     */
    @Test
    public void testRuleContainer() {
        SSMRule ruleObject = SSMRuleParser.parseAll("file.path matches('/A/[a-z]*') : accessCount(10 min) >= 10 | cache").get();
        //second
        long updateDuration = 1 * 60;
        DFSClient dfsClient = null;
        RuleContainer ruleContainer = new RuleContainer(ruleObject, updateDuration, dfsClient);
        String fileNameA = "/A/a";
        String fileNameB = "/A/b";
        String fileNameC = "/A/c";
        String fileNameD = "/A/d";
        String fileNameE = "/A/e";
        String fileNameEe = "/A/ee";
        String fileNameF = "/A/f";
        FileAccess fileAccessA = new FileAccess(fileNameA, 10);
        FileAccess fileAccessB = new FileAccess(fileNameB, 11);
        FileAccess fileAccessC = new FileAccess(fileNameC, 2);
        FileAccess fileAccessD = new FileAccess(fileNameD, 13);
        FileAccess fileAccessE = new FileAccess(fileNameE, 14);
        FileAccess fileAccessF = new FileAccess(fileNameF, 15);
        FileAccess fileAccessEe = new FileAccess(fileNameEe, 14);
        FileAccessMap fileAccessMapA = new FileAccessMap();
        fileAccessMapA.put(fileNameA, fileAccessA);
        FileAccessMap fileAccessMapB = new FileAccessMap();
        fileAccessMapB.put(fileNameB, fileAccessB);
        FileAccessMap fileAccessMapC = new FileAccessMap();
        fileAccessMapC.put(fileNameC, fileAccessC);
        FileAccessMap fileAccessMapD = new FileAccessMap();
        fileAccessMapD.put(fileNameD, fileAccessD);
        FileAccessMap fileAccessMapE = new FileAccessMap();
        fileAccessMapE.put(fileNameE, fileAccessE);
        FileAccessMap fileAccessMapF = new FileAccessMap();
        fileAccessMapF.put(fileNameF, fileAccessF);
        ArrayList<Map<String, Integer>> listMap = new ArrayList<>();
        Map<String, Integer> mapA = new HashMap<String, Integer>();
        mapA.put(fileNameA, 10);
        listMap.add(mapA);
        Map<String, Integer> mapB = new HashMap<String, Integer>();
        mapB.put(fileNameB, 11);
        listMap.add(mapB);
        Map<String, Integer> mapC = new HashMap<String, Integer>();
        mapC.put(fileNameC, 2);
        listMap.add(mapC);
        Map<String, Integer> mapD = new HashMap<String, Integer>();
        mapD.put(fileNameD, 13);
        listMap.add(mapD);
        Map<String, Integer> mapE = new HashMap<String, Integer>();
        mapE.put(fileNameE, 14);
        listMap.add(mapE);
        Map<String, Integer> mapF = new HashMap<String, Integer>();
        mapF.put(fileNameF, 15);
        listMap.add(mapF);
        NNEvent nnEventA = new NNEvent(0);
        NNEvent nnEventB = new NNEvent(0);
        NNEvent nnEventC = new NNEvent(0);
        NNEvent nnEventD = new NNEvent(1, fileNameB);
        NNEvent nnEventE = new NNEvent(2, fileAccessE.getFileName(), "/A/ee");
        NNEvent nnEventF = new NNEvent(2, fileAccessF.getFileName(), "/A/F");
        List<List<NNEvent>> listList = new ArrayList<List<NNEvent>>();
        List<NNEvent> nnEventListA = new ArrayList<NNEvent>();
        nnEventListA.add(nnEventA);
        listList.add(nnEventListA);
        List<NNEvent> nnEventListB = new ArrayList<NNEvent>();
        nnEventListB.add(nnEventB);
        listList.add(nnEventListB);
        List<NNEvent> nnEventListC = new ArrayList<NNEvent>();
        nnEventListC.add(nnEventC);
        listList.add(nnEventListC);
        List<NNEvent> nnEventListD = new ArrayList<NNEvent>();
        nnEventListD.add(nnEventD);
        listList.add(nnEventListD);
        List<NNEvent> nnEventListE = new ArrayList<NNEvent>();
        nnEventListE.add(nnEventE);
        listList.add(nnEventListE);
        List<NNEvent> nnEventListF = new ArrayList<NNEvent>();
        nnEventListF.add(nnEventF);
        listList.add(nnEventListF);
        FilesAccessInfo filesAccessInfoA = new FilesAccessInfo();
        FilesAccessInfo filesAccessInfoB = new FilesAccessInfo();
        FilesAccessInfo filesAccessInfoC = new FilesAccessInfo();
        FilesAccessInfo filesAccessInfoD = new FilesAccessInfo();
        FilesAccessInfo filesAccessInfoE = new FilesAccessInfo();
        FilesAccessInfo filesAccessInfoF = new FilesAccessInfo();
        filesAccessInfoA.setAccessCounter(listMap.get(0));
        filesAccessInfoA.setNnEvents(listList.get(0));
        filesAccessInfoB.setAccessCounter(listMap.get(1));
        filesAccessInfoB.setNnEvents(listList.get(1));
        filesAccessInfoC.setAccessCounter(listMap.get(2));
        filesAccessInfoC.setNnEvents(listList.get(2));
        filesAccessInfoD.setAccessCounter(listMap.get(3));
        filesAccessInfoD.setNnEvents(listList.get(3));
        filesAccessInfoE.setAccessCounter(listMap.get(4));
        filesAccessInfoE.setNnEvents(listList.get(4));
        filesAccessInfoF.setAccessCounter(listMap.get(5));
        filesAccessInfoF.setNnEvents(listList.get(5));
        HashMap<String, Action> hm = new HashMap<String, Action>();
        //first put
        ruleContainer.update(filesAccessInfoB);
        hm = ruleContainer.actionEvaluator(fileAccessMapB);
        //B
        assertEquals(0, ruleContainer.getWindowMap().getWindowMaps().getLast().get(fileNameB).compareTo(fileAccessB));
        //except for the first time in the first ten times And evaluator isn't work.tip,evaluator 10 times after operation
        for (int i = 1; i < 10; i++) {
            ruleContainer.update(filesAccessInfoA);
            ruleContainer.actionEvaluator(fileAccessMapA);
        }
        //BAAAAAAAAA
        assertEquals(0, ruleContainer.getWindowMap().getWindowMaps().getFirst().get(fileNameB).compareTo(fileAccessB));
        assertEquals(0, ruleContainer.getWindowMap().getWindowMaps().getLast().get(fileNameA).compareTo(fileAccessA));
        //+B AAAAAAAAAB test only contain 10 steps and account
        ruleContainer.update(filesAccessInfoB);
        hm = ruleContainer.actionEvaluator(fileAccessMapB);
        assertEquals(0, ruleContainer.getWindowMap().getWindowMaps().getFirst().get(fileNameA).compareTo(fileAccessA));
        assertEquals(0, ruleContainer.getWindowMap().getWindowMaps().getLast().get(fileNameB).compareTo(fileAccessB));
        assertEquals(0, ruleContainer.getWindowMap().getFileAccessMapInWindow().get(fileNameB).getAccessCount().compareTo(11));
        assertEquals(Action.CACHE, hm.get(fileNameB));
        //AAAAAAAABE
        ruleContainer.update(filesAccessInfoE);
        hm = ruleContainer.actionEvaluator(fileAccessMapE);
        assertEquals(0, ruleContainer.getWindowMap().getWindowMaps().getLast().get(fileNameEe).compareTo(fileAccessEe));
        assertEquals(Action.CACHE, hm.get(fileNameEe));
        //AAAAAAABE(new empty map)
        ruleContainer.update(filesAccessInfoF);
        hm = ruleContainer.actionEvaluator(fileAccessMapF);
        assertEquals(true, ruleContainer.getWindowMap().getWindowMaps().getLast().getMap().isEmpty());
        //AAAAAAAABE(new empty map)
        ruleContainer.update(filesAccessInfoC);
        ruleContainer.actionEvaluator(fileAccessMapC);
        for (int j = 0; j < 8; j++) {
            ruleContainer.update(filesAccessInfoA);
            ruleContainer.actionEvaluator(fileAccessMapA);
        }
        ruleContainer.update(filesAccessInfoA);
        hm = ruleContainer.actionEvaluator(fileAccessMapA);
        assertEquals(false, hm.containsKey(fileNameC));
        assertEquals(true, ruleContainer.getWindowMap().getFileAccessMapInWindow().containsKey(fileNameC));
        //AAAAAAAAED
        ruleContainer.update(filesAccessInfoD);
        ruleContainer.actionEvaluator(fileAccessMapD);
        assertEquals(false, ruleContainer.getWindowMap().getWindowMaps().contains(fileAccessMapB));
        assertEquals(false, ruleContainer.getWindowMap().getFileAccessMapInWindow().containsKey(fileNameB));
    }


    @Test
    public void testContainerMax_Map_Num() {
        SSMRule ruleObject = SSMRuleParser.parseAll("file.path matches('/A/[a-z]*') : accessCount(40 min) >= 1 | cache").get();
        //second
        long updateDuration = 1 * 60;
        DFSClient dfsClient = null;
        RuleContainer ruleContainer = new RuleContainer(ruleObject, updateDuration, dfsClient);
        String fileNameA = "/A/a";
        String fileNameB = "/A/b";
        String fileNameC = "/A/c";
        String fileNameD = "/A/d";
        FileAccess fileAccessA = new FileAccess(fileNameA, 10);
        FileAccess fileAccessB = new FileAccess(fileNameB, 11);
        FileAccess fileAccessC = new FileAccess(fileNameC, 12);
        FileAccess fileAccessD = new FileAccess(fileNameD, 13);
        FileAccessMap fileAccessMapA = new FileAccessMap();
        fileAccessMapA.put(fileNameA, fileAccessA);
        FileAccessMap fileAccessMapB = new FileAccessMap();
        fileAccessMapB.put(fileNameB, fileAccessB);
        FileAccessMap fileAccessMapC = new FileAccessMap();
        fileAccessMapC.put(fileNameC, fileAccessC);
        FileAccessMap fileAccessMapD = new FileAccessMap();
        fileAccessMapD.put(fileNameD, fileAccessD);
        ArrayList<Map<String, Integer>> listMap = new ArrayList<>();
        Map<String, Integer> mapA = new HashMap<String, Integer>();
        mapA.put(fileNameA, 10);
        listMap.add(mapA);
        Map<String, Integer> mapB = new HashMap<String, Integer>();
        mapB.put(fileNameB, 11);
        listMap.add(mapB);
        Map<String, Integer> mapC = new HashMap<String, Integer>();
        mapC.put(fileNameC, 12);
        listMap.add(mapC);
        Map<String, Integer> mapD = new HashMap<String, Integer>();
        mapD.put(fileNameD, 13);
        listMap.add(mapD);
        NNEvent nnEventA = new NNEvent(0);
        NNEvent nnEventB = new NNEvent(0);
        NNEvent nnEventC = new NNEvent(0);
        NNEvent nnEventD = new NNEvent(0);
        List<List<NNEvent>> listList = new ArrayList<List<NNEvent>>();
        List<NNEvent> nnEventListA = new ArrayList<NNEvent>();
        nnEventListA.add(nnEventA);
        listList.add(nnEventListA);
        List<NNEvent> nnEventListB = new ArrayList<NNEvent>();
        nnEventListB.add(nnEventB);
        listList.add(nnEventListB);
        List<NNEvent> nnEventListC = new ArrayList<NNEvent>();
        nnEventListC.add(nnEventC);
        listList.add(nnEventListC);
        List<NNEvent> nnEventListD = new ArrayList<NNEvent>();
        nnEventListD.add(nnEventD);
        listList.add(nnEventListD);
        FilesAccessInfo filesAccessInfoA = new FilesAccessInfo();
        FilesAccessInfo filesAccessInfoB = new FilesAccessInfo();
        FilesAccessInfo filesAccessInfoC = new FilesAccessInfo();
        FilesAccessInfo filesAccessInfoD = new FilesAccessInfo();
        filesAccessInfoA.setAccessCounter(listMap.get(0));
        filesAccessInfoA.setNnEvents(listList.get(0));
        filesAccessInfoB.setAccessCounter(listMap.get(1));
        filesAccessInfoB.setNnEvents(listList.get(1));
        filesAccessInfoC.setAccessCounter(listMap.get(2));
        filesAccessInfoC.setNnEvents(listList.get(2));
        filesAccessInfoD.setAccessCounter(listMap.get(3));
        filesAccessInfoD.setNnEvents(listList.get(3));

        HashMap<String, Action> hm = new HashMap<String, Action>();
        //put D
        ruleContainer.update(filesAccessInfoD);
        hm = ruleContainer.actionEvaluator(fileAccessMapD);
        //a step is 2minute
        assertEquals(false,ruleContainer.getWindowMap().getWindowMaps().contains(fileAccessMapD));
        //DA
        ruleContainer.update(filesAccessInfoA);
        hm = ruleContainer.actionEvaluator(fileAccessMapA);
        assertEquals(true,ruleContainer.getWindowMap().getWindowMaps().getFirst().containsKey(fileNameD));
        assertEquals(true,ruleContainer.getWindowMap().getWindowMaps().getFirst().containsKey(fileNameA));
        //(D)ABBBBBBBBBBBBBBBBBBC
        for(int i=0; i<18;i++ ){
            ruleContainer.update(filesAccessInfoB);
            hm = ruleContainer.actionEvaluator(fileAccessMapB);
        }
        ruleContainer.update(filesAccessInfoC);
        hm = ruleContainer.actionEvaluator(fileAccessMapC);
        assertEquals(false,ruleContainer.getWindowMap().getWindowMaps().contains(fileAccessMapD));
        //contain 20map
        assertEquals(0,ruleContainer.getWindowMap().getWindowMaps().getFirst().get(fileNameA).compareTo(fileAccessA));
        assertEquals(0,ruleContainer.getWindowMap().getWindowMaps().getLast().get(fileNameC).compareTo(fileAccessC));
    }


}
