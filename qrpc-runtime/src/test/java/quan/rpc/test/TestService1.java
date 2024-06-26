package quan.rpc.test;

import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quan.rpc.Endpoint;
import quan.rpc.Promise;
import quan.rpc.ProxyConstructors;
import quan.rpc.Service;
import quan.rpc.Timer;

import java.lang.annotation.ElementType;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static quan.rpc.ProxyConstructors.NODE_ID;
import static quan.rpc.ProxyConstructors.NODE_ID_AND_SERVICE_ID;
import static quan.rpc.ProxyConstructors.NODE_ID_RESOLVER_AND_SERVICE_ID;
import static quan.rpc.ProxyConstructors.NO_ARGS;
import static quan.rpc.ProxyConstructors.SERVICE_ID;

/**
 * 测试服务1
 */
@ProxyConstructors({NO_ARGS, NODE_ID, SERVICE_ID, NODE_ID_AND_SERVICE_ID, NODE_ID_RESOLVER_AND_SERVICE_ID})
public class TestService1 extends Service<Integer> {

    private static Logger logger = LoggerFactory.getLogger(TestService1.class);

    private int id;

    private long lastTime;

    private int x;

    private RoleService2Proxy<?> roleService2Proxy = new RoleService2Proxy<>(2L);

    private TestService2Proxy testService2Proxy = new TestService2Proxy(2, 2);


    public TestService1(int id) {
        this.id = id;
    }

    @Override
    public Integer getId() {
        return id;
    }


    @Override
    protected void init() {
        logger.info("TestService1:{} init at worker:{},thread:{}", this.id, this.getWorker().getId(), Thread.currentThread());
        newTimer(() -> {
            System.err.println("cron timer execute,time：" + getTime() + ",thread:" + Thread.currentThread());
        }, "0/10 * * * * ? ");

        newTimer(this::update, 100, 100);

        for (int i = 0; i < 100; i++) {
            int a = i;
            newTimer(() -> {
                for (int j = 0; j < 1; j++) {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException ignored) {
                    }
                }
                x = RandomUtils.nextInt() + a;
            }, 100, 100);
        }

    }

    @Override
    protected void destroy() {
        logger.info("TestService1:{} destroy at worker{}", this.id, this.getWorker().getId());
    }

    protected void update() {
        long now = System.currentTimeMillis();

        synchronized (this) {
            if (now - lastTime < 5000) {
                return;
            }
            lastTime = now;
        }

        logger.info("TestService1:{} call RoleService1 at worker{}", this.id, this.getWorker().getId());

        int a = (int) (now % 3);
        int b = (int) (now % 10);
        long startTime = System.nanoTime();

        Promise<Integer> promise = roleService2Proxy.login1(a, b, 111L, 333L);
        promise.thenCompose(result1 -> {
            double costTime = (System.nanoTime() - startTime) / 1000000D;
            logger.info("TestService1:{} call RoleService1.login1({},{})={},costTime:{}", this.id, a, b, result1, costTime);
            return roleService2Proxy.login2(a, b + 1);//result2
        }).thenCompose(result2 -> {
            logger.info("TestService1 call RoleService1.login2,result2:{}", result2);
            return roleService2Proxy.login3(a, b + 2, null);//result3
        }).thenAccept(result3 -> {
            logger.info("TestService1 call RoleService1.login3,result3:{}", result3);
        }).exceptionally(e -> {
            logger.error(" TestService1 call RoleService1 error", e);
        });

        Promise<Integer> countPromise = roleService2Proxy.count(new HashSet<>(Arrays.asList(2334, 664)));
        countPromise.thenAccept(r -> {
            logger.info("1:roleService1Proxy.count()={}", r);
        }).completely(() -> {
            logger.error("3:roleService1Proxy.count()");
        }).thenAccept(r -> {
            logger.info("4:roleService1Proxy.count()={}", r);
        }).thenAccept(r -> {
            logger.info("5:roleService1Proxy.count()={}", r);
        }).exceptionally(e -> {
            logger.error("6:roleService1Proxy.count()", e);
        });

    }


    /**
     * a+b
     */
    @Endpoint
    public Promise<Integer> add1(Integer a, Integer b) {
        int r = a + b;
        logger.info("Execute TestService1:{}.add1({},{})={} at Worker:{}", id, a, b, r, this.getWorker().getId());
        Promise<Integer> promise = testService2Proxy.size(Arrays.asList(1, 2, 4));

        promise.thenCompose(s -> testService2Proxy.add3(r, a))
                .thenAccept(r3 -> {
                    logger.info("TestService1:{} call TestService2.add3({},{})={}", id, r, a, r3);
                });

        return promise;
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

    @Endpoint
    public void type(ElementType type) {
    }

    @Timer.Period(1000)
    private void save() {
        logger.error("save {}", System.currentTimeMillis());
    }

}
