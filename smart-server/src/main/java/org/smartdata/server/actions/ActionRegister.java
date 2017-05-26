package org.smartdata.server.actions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.common.actions.ActionType;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Provide Action Map and Action instance
 */
public class ActionRegister {
  static final Logger LOG = LoggerFactory.getLogger(ActionRegister.class);

  private static ActionRegister instance = new ActionRegister();
  HashMap<String, Class> actionMap;

  synchronized public static ActionRegister getInstance() {
    return instance;
  }

  private ActionRegister() {
    actionMap = new HashMap<>();
  }

  public void initial() {
    loadNativeAction();
    loadUserDefinedAction();
  }

  public void loadNativeAction() {
    String path = "org.smartdata.server.actions.";
    for (ActionType t : ActionType.values()) {
      String name = t.toString();
      try {
        actionMap.put(name, Class.forName(path + name));
      } catch (ClassNotFoundException e) {
        LOG.info("Class {} not found!", name);
        continue;
        // actionMap.put(name, null);
      }
    }
  }

  public void loadUserDefinedAction() {
    // TODO Replace with Dir
    String pathToJar = "/home/intel/Develop/SSM/Smart/User/UDAction.jar";
    try {
      JarFile jarFile = new JarFile(pathToJar);
      Enumeration<JarEntry> jarEnties = jarFile.entries();
      URL[] urls = {new URL("jar:file:" + pathToJar + "!/")};
      URLClassLoader cl = URLClassLoader.newInstance(urls);
      while (jarEnties.hasMoreElements()) {
        JarEntry je = jarEnties.nextElement();
        if (je.isDirectory() || !je.getName().endsWith(".class")) {
          continue;
        }
        // -6 because of .class
        String className = je.getName().substring(0, je.getName().length() - 6);
        LOG.info(className);
        className = className.replace('/', '.');
        String actionName = className.substring(className.lastIndexOf('.') + 1);
        Class c = cl.loadClass(className);
        actionMap.put(actionName, c);
      }
    } catch (Exception e) {
      LOG.error(e.getMessage());
      LOG.error("Load from Classes JAR error!");
    }
  }

  public boolean checkAction(String name) {
    return actionMap.containsKey(name);
  }

  public String[] namesOfAction() {
    return actionMap.keySet().toArray(new String[actionMap.size()]);
  }

  public Action newActionFromName(String name) {
    try {
      return (Action) actionMap.get(name).newInstance();
    } catch (Exception e) {
      LOG.info("New {} Action Error!", name);
      LOG.error(e.getMessage());
    }
    return null;
  }
}
