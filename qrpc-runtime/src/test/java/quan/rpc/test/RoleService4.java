package quan.rpc.test;

import quan.rpc.Endpoint;

/**
 * @author quanchangnai
 */
public class RoleService4 extends RoleService3<Integer, Runnable> {

    public RoleService4(long id) {
        super(id);
    }

    @Endpoint
    @Override
    public String getName() {
        return super.getName();
    }

    @Endpoint
    @Override
    public int getLevel() {
        return 4;
    }

    @Endpoint
    public String f4() {
        return "f4";
    }

    @Override
    public String toString() {
        return "RoleService4{"
                + "id:" + getId()
                + ",name:" + getName()
                + ",level:" + getLevel()
                + "}";
    }
}
