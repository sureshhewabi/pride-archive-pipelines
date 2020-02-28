package uk.ac.ebi.pride.archive.pipeline.utility;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisCluster;
import uk.ac.ebi.pride.integration.command.builder.CommandBuilder;
import uk.ac.ebi.pride.integration.command.builder.DefaultCommandBuilder;

@Slf4j
public class RedisUtilities {

  /**
   * Gets a String value for the specified Redis key.
   *
   * @param key the key to use
   * @return String the key's value
   */
  public static String getStringFromRedis(String key, JedisCluster jedisCluster) {
    String result = "";
    try {
      log.info("Connecting to Redis.");
      log.info("Getting contents to Redis for key: " + key);
      if (!jedisCluster.exists(key)) {
        log.error("Redis does not hold a value for key: " + key);
      } else {
        result = jedisCluster.get(key);
        log.info("Successfully gotten value from key: " + result);
      }
    } catch (Exception e) {
      log.error("Exception while getting value to Redis for key: " + key, e);
    }
    return result;
  }

  /**
   * Removes a key (or more) and the corresponding value(s) from Redis.
   *
   * @param keyToRemove the key to remove
   */
  public static void removeKeyAndValueInRedis(String keyToRemove, JedisCluster jedisCluster) {
    try {
      log.info("Connecting to Redis.");
      log.info("Removing Redis key: " + keyToRemove);
      long numberOfKeysRemoved = jedisCluster.del(keyToRemove);
      if (numberOfKeysRemoved < 1) {
        log.info("Redis does not hold a value for key: " + keyToRemove);
      } else {
        log.info("Successfully removed key: " + keyToRemove);
      }
    } catch (Exception e) {
      log.error("Exception while removing in Redis for key: " + keyToRemove, e);
    }
  }
}
