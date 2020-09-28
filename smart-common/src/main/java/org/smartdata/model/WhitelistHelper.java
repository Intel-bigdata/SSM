package org.smartdata.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.conf.SmartConf;

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
     * @return
     */
    public boolean isEnabled(SmartConf conf) {
        return !conf.getCoverDir().isEmpty();
    }

    /**
     * Check if the work path in the whitelist.
     * @param path
     * @param conf
     */
    public void checkPath(String path, SmartConf conf) {
        String filePath = path.endsWith("/") ? path : path + "/";
        for (String s : conf.getCoverDir()) {
            if (filePath.startsWith(s)) {
                LOG.debug("Path " + filePath + " is in whitelist.");
                return;
            }
        }
        throw new IllegalArgumentException("Path "
                + filePath + " is not in whitelist.");
    }
}
