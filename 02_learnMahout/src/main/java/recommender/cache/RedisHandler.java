package recommender.cache;


import recommender.utils.MyShardedJedisPool;

/**
 * Describe: 请补充类描述
 */
public class RedisHandler {
    public static String getValueByHashField(String key, String field) {
        return MyShardedJedisPool.getResource().hget(key, field);
    }

    public static String getString(String key) {
        return MyShardedJedisPool.getResource().get(key);
    }
}
