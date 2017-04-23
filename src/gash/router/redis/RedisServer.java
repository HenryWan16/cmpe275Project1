package gash.router.redis;

/**
 * Created by nguyen on 4/22/17.
 */

import redis.clients.jedis.Jedis;
public class RedisServer {
    private Jedis localhostJedis;
    private static RedisServer instance;
    private RedisServer(){
        this.localhostJedis = new Jedis("localhost",6379);
    }

    public static RedisServer getInstance(){
        if(instance == null){
            instance = new RedisServer();
        }
        return instance;
    }

    public Jedis getLocalhostJedis(){
        return localhostJedis;
    }
}
