package org.smartdata.server.actions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.common.actions.ActionType;

import java.util.HashMap;

/**
 * Provide Action Map and Action instance
 */
public class ActionRegister {
  static final Logger LOG = LoggerFactory.getLogger(ActionRegister.class);

  static ActionRegister instance = new ActionRegister();
  HashMap<String, Class> actionMap;

  public ActionRegister getInstance() {
    return instance;
  }

  private ActionRegister() {
    actionMap = new HashMap<>();
    for (ActionType t : ActionType.values()) {
      String name = t.toString();
      try {
        actionMap.put(name, Class.forName(name));
      } catch (ClassNotFoundException e) {
        LOG.info("Class {} not found!", name);
        actionMap.put(name, null);
      }
    }
    // TODO Load Class from Path
//    String pathToJar = "Smart/User/";
//    try {
//      JarFile jarFile = new JarFile(pathToJar);
//      Enumeration<JarEntry> jarEnties = jarFile.entries();
//      URL[] urls = {new URL("jar:file:" + pathToJar + "!/")};
//      URLClassLoader cl = URLClassLoader.newInstance(urls);
//      while (jarEnties.hasMoreElements()) {
//        JarEntry je = jarEnties.nextElement();
//        if(je.isDirectory() || !je.getName().endsWith(".class")){
//          continue;
//        }
//        // -6 because of .class
//        String className = je.getName().substring(0,je.getName().length() - 6);
//        className = className.replace('/', '.');
//        Class c = cl.loadClass(className);
//        actionMap.put(className, c);
//      }
//    } catch (Exception e) {
//      LOG.error(e.getMessage());
//      LOG.error("Load from Classes JAR error!");
//    }
  }

  public boolean checkAction(String name) {
    return actionMap.containsKey(name);
  }

  public String[] namesofAction() {
    return actionMap.keySet().toArray(new String[actionMap.size()]);
  }

  public Action newActionFromName(String name) {
    try {
      return (Action) actionMap.get(name).newInstance();
    } catch (Exception e) {
      LOG.error(e.getMessage());
    }
    return null;
  }
}
