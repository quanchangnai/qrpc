package quan.rpc.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quan.rpc.Endpoint;
import quan.rpc.UpdatableService;
import quan.rpc.Promise;

import java.util.*;

/**
 * run
 */
public class TestService1 extends UpdatableService {

    private static Logger logger = LoggerFactory.getLogger(TestService1.class);

    private int id;

    private long lastTime;

    private RoleService1Proxy<?> roleService1Proxy = new RoleService1Proxy<>(2L);

    private TestService2Proxy testService2Proxy = new TestService2Proxy(2, 2);


    public TestService1(int id) {
        this.id = id;
    }

    @Override
    public Object getId() {
        return id;
    }

    /**
     * a+b
     */
    @Endpoint
    public int add1(Integer a, Integer b) {
        int r = a + b;
        logger.info("Execute TestService1:{}.add1({},{})={} at Worker:{}", id, a, b, r, this.getWorker().getId());
        Promise<Integer> promise = testService2Proxy.add3(r, a);
        promise.then(r3 -> {
            logger.info("TestService1:{} call TestService2.add3({},{})={}", id, a, b, r3);
        });
        return r;
    }

    @Endpoint
    public int add2(Integer a, Integer b) {
        int r = a + b;
        logger.info("Execute TestService1:{}.add2({},{})={} at Worker:{}", id, a, b, r, this.getWorker().getId());
        return r;
    }

    @Endpoint
    public void remove(Map<Integer, String> map, int a) {
        map.remove(a);
        logger.info("Execute TestService1:{}.remove({}) at Worker:{}", id, a, this.getWorker().getId());
    }

    @Endpoint
    public <E> Integer size(List<? super Runnable> list) {
        return list.size();
    }

    @Endpoint
    public Integer size(Set<?> set) {
        return set.size();
    }


    @Override
    protected void init() {
        logger.info("TestService1:{} init at worker{}", this.id, this.getWorker().getId());
    }

    @Override
    protected void destroy() {
        logger.info("TestService1:{} destroy at worker{}", this.id, this.getWorker().getId());
    }

    @Override
    protected void update() {
        long now = System.currentTimeMillis();
        if (lastTime > 0 && now < lastTime + 5000) {
            return;
        }
        lastTime = now;

        logger.info("TestService1:{} call RoleService1 at worker{}", this.id, this.getWorker().getId());

        int a = (int) (now % 3);
        int b = (int) (now % 10);
        long startTime = System.nanoTime();

        Promise<Integer> promise = roleService1Proxy.login1(a, b, 111L, 333L);
        promise.then(result1 -> {
            double costTime = (System.nanoTime() - startTime) / 1000000D;
            logger.info("TestService1:{} call RoleService1.login1({},{})={},costTime:{}", this.id, a, b, result1, costTime);
            return roleService1Proxy.login2(a, b + 1);//result2
        }).then(result2 -> {
            logger.info("TestService1 call RoleService1.login2,result2:{}", result2);
            return roleService1Proxy.login3(a, b + 2);//result3
        }).then(result3 -> {
            logger.info("TestService1 call RoleService1.login3,result3:{}", result3);
        });

        Promise<Integer> count = roleService1Proxy.count(new HashSet<>(Arrays.asList(2334, 664)));
        count.then(c -> {
            logger.info("roleService1Proxy.count()={}", c);
        });

    }

}
