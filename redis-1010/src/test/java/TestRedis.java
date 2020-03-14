import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Protocol;

public class TestRedis {

    private String ipAddress = "192.168.233.102";

    private int port = Protocol.DEFAULT_PORT;

    @Test
    public void test() {

        Jedis jedis = new Jedis(ipAddress, port);

        System.out.println(jedis.ping());

    }


    @Test
    public void testPool() {

        JedisPool jedisPool = new JedisPool(ipAddress, port);

        Jedis jedis = jedisPool.getResource();

        System.out.println(jedis.get("aaa"));


    }

}
