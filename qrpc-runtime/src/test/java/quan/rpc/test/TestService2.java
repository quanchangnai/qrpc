package quan.rpc.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quan.rpc.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 测试服务2
 *
 * @author quanchangnai
 */
public class TestService2 extends Service<Integer> {

    private static Logger logger = LoggerFactory.getLogger(TestService2.class);

    private int id;

    private NodeIdResolver nodeIdResolver = new NodeIdResolver() {

        int count = 0;

        @Override
        public int resolveNodeId(Proxy proxy) {
            if (count < 2) {
                count++;
                return -1;
            } else {
                count = 0;
                return 1;
            }
        }
    };

    private TestService1Proxy testService1Proxy = new TestService1Proxy(nodeIdResolver, 1);

    public TestService2(int id) {
        this.id = id;
    }

    @Override
    public Integer getId() {
        return id;
    }

    @Override
    protected void init() {
        newTimer(this::add, 1000, 5000);
    }

    /**
     * a+b
     */
    @Endpoint
    public int add1(Integer a, Integer b) {
        int r = a + b;
        logger.info("Execute TestService2:{}.add1({},{})={} at Worker:{}", id, a, b, r, this.getWorker().getId());
        return r;
    }

    @Endpoint
    public int add2(Integer a, Integer b) {
        int r = a + b;
        logger.info("Execute TestService2:{}.add2({},{})={} at Worker:{}", id, a, b, r, this.getWorker().getId());
        return r;
    }

    @Endpoint
    public Promise<Integer> add3(Integer a, Integer b) {
        int r = a + b;
        logger.info("Execute TestService2:{}.add3({},{})={} at Worker:{}", id, a, b, r, this.getWorker().getId());
        DelayedResult<Integer> delayedResult = newDelayedResult();
        execute(() -> {
            delayedResult.setResult(r);
        });
        return delayedResult;
    }

    @Endpoint
    public void remove(Map<Integer, String> map, int a) {
        map.remove(a);
        logger.info("Execute TestService2:{}.remove({}) at Worker:{}", id, a, this.getWorker().getId());
    }

    @Endpoint
    public <E> Integer size(Set<? super Runnable> set) {
        logger.info("Execute TestService2:{}.size({}) at Worker:{}", id, set, this.getWorker().getId());
        return set.size();
    }

    @Endpoint
    public <E> Integer size(List<Integer> list) {
        logger.info("Execute TestService2:{}.size({}) at Worker:{}", id, list, this.getWorker().getId());
        return list.size();
    }


    private void add() {
        logger.info("TestService2:{} call TestService1 at Worker:{}", this.id, this.getWorker().getId());

        long startTime = System.nanoTime();
        int a = (int) (startTime % 3);
        int b = (int) (startTime % 10);

        Promise<Integer> promise = testService1Proxy.add1(a, b);
        promise.then(result -> {
            double costTime = (System.nanoTime() - startTime) / 1000000D;
            logger.info("TestService2:{} call TestService1.add1({},{})={},costTime:{}", this.id, a, b, result, costTime);
            System.err.println();
        });

        add3(a, b).then(r -> {
            logger.info("1:add3({}, {})={}", a, b, r);
            return add3(a, r);
        }).then(r -> {
            logger.info("2:add3({}, {})={}", a, (r - a), r);
            return testService1Proxy.add1(a, r);
        }).then(r -> {
            logger.info("3:add3({}, {})={}", a, (r - a), r);
            return add3(a, r);
        }).then(r -> {
            logger.info("4:add3({}, {})={}", a, (r - a), r);
        });
    }

}
