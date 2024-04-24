package quan.rpc.test;

import quan.rpc.Endpoint;
import quan.rpc.ProxyConstructors;
import quan.rpc.Service;

import static quan.rpc.ProxyConstructors.NODE_ID;
import static quan.rpc.ProxyConstructors.NODE_ID_AND_SERVICE_ID;
import static quan.rpc.ProxyConstructors.NODE_ID_AND_SHARDING_KEY;
import static quan.rpc.ProxyConstructors.NODE_ID_RESOLVER;
import static quan.rpc.ProxyConstructors.NODE_ID_RESOLVER_AND_SERVICE_ID;
import static quan.rpc.ProxyConstructors.NO_ARGS;
import static quan.rpc.ProxyConstructors.SERVICE_ID;
import static quan.rpc.ProxyConstructors.SHARDING_KEY;
import static quan.rpc.ProxyConstructors.SHARDING_KEY_AND_SERVICE_ID;


/**
 * @author quanchangnai
 */
@ProxyConstructors({NO_ARGS, NODE_ID, SERVICE_ID, NODE_ID_AND_SERVICE_ID, NODE_ID_RESOLVER, NODE_ID_RESOLVER_AND_SERVICE_ID, SHARDING_KEY, NODE_ID_AND_SHARDING_KEY, SHARDING_KEY_AND_SERVICE_ID})
public abstract class RoleService1 extends Service<Long> {

    @Override
    public Long getId() {
        return null;
    }

    @Endpoint
    public String getName() {
        return "aaa";
    }

    @Endpoint
    public int getLevel() {
        return 1;
    }


}
