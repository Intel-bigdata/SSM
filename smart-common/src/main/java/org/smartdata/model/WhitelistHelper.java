package org.smartdata.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.conf.SmartConf;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * It's a helper for whitelist function. It's used in action and rule submit process.
 * Especially, SmallFileScheduler also use this helper for  distinguishing between
 * whitelist check and invalid small files exception.
 */
public class WhitelistHelper {
    private static final Logger LOG = LoggerFactory.getLogger(WhitelistHelper.class);

    /**
     * Check if whitelist is enabled.
     * @param conf
     * @return true/false
     */
    public static boolean isEnabled(SmartConf conf) {
        return !conf.getCoverDir().isEmpty();
    }

    /**
     * Check if the work path in the whitelist.
     * @param path
     * @param conf
     * @return true/false
     */
    public static boolean isInWhitelist(String path, SmartConf conf) {
        String filePath = path.endsWith("/") ? path : path + "/";
        for (String s : conf.getCoverDir()) {
            if (filePath.startsWith(s)) {
                LOG.debug("Path " + filePath + " is in whitelist.");
                return true;
            }
        }
        return false;
    }

    /**
     *  Check if  cmdlet in the whitelist.
     * @param cmdletDescriptor
     * @return true/false
     */
    public static boolean isCmdletInWhitelist(CmdletDescriptor cmdletDescriptor) {
        SmartConf conf = new SmartConf();
        int size = cmdletDescriptor.getActionSize();
        for (int index = 0; index < size; index++) {
            String actionName = cmdletDescriptor.getActionName(index);
            Map<String, String> args = cmdletDescriptor.getActionArgs(index);
            //check in the SmallFileScheduler for small file action
            if (actionName.equals("compact") || actionName.equals("uncompact")) {
                continue;
            } else if (args.containsKey(CmdletDescriptor.HDFS_FILE_PATH)) {
                String filePath = args.get(CmdletDescriptor.HDFS_FILE_PATH);
                LOG.debug("WhiteList helper is checking path: " + filePath);
                if (!isInWhitelist(filePath, conf)) {
                    return false;
                }
            } else {
                LOG.debug("This action text doesn't contain file path.");
            }
        }
        return true;
    }

    /**
     * Check if white list changed, it influences namespace fetch process.
     * @param conf
     * @return true/false
     */
    public static boolean isWhitelistChanged(SmartConf conf) {
        List<String> currentList = conf.getCoverDir();
        List<String> oldList = conf.getLastFetchList();
        if (currentList.size() != oldList.size()) {
            return true;
        }
        Set<String> set = new HashSet<>();
        for (String s : oldList) {
            set.add(s);
        }
        for (String s : currentList) {
            if (!set.contains(s)) {
                return true;
            }
        }
        return false;
    }
}
