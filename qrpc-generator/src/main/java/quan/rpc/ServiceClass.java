package quan.rpc;

import java.util.*;
import java.util.stream.Collectors;

public class ServiceClass extends ServiceElement {

    private final String fullName;

    private String packageName;

    //是不是抽象类
    private boolean _abstract;

    private final List<ServiceMethod> methods = new ArrayList<>();

    //同名方法的数量
    private final Map<String, Integer> sameNameMethodCounts = new HashMap<>();

    //服务ID的类型
    private String idType;

    /**
     * 服务代理支持的构造方法，参考{@link Proxy}的6个构造方法
     */
    private Set<Integer> proxyConstructors = new HashSet<>();

    //父类
    private String superName;

    //父类的泛型的类型参数
    protected String superTypeParameters;

    //自定义代理代码的生成路径
    private boolean customProxyPath;

    public ServiceClass(String fullName) {
        this.fullName = fullName;
        int index = fullName.lastIndexOf(".");
        if (index > 0) {
            this.packageName = fullName.substring(0, index);
            this.name = fullName.substring(index + 1);
        } else {
            this.name = fullName;
        }
        this.serviceClass = this;
    }

    public String getFullName() {
        return fullName;
    }

    public String getPackageName() {
        return packageName;
    }

    public boolean isAbstract() {
        return _abstract;
    }

    public void setAbstract(boolean _abstract) {
        this._abstract = _abstract;
    }

    public List<ServiceMethod> getMethods() {
        return methods;
    }

    public List<ServiceMethod> getMethods(boolean hasExpiredTime) {
        return methods.stream().filter(m -> hasExpiredTime == (m.getExpiredTime() > 0)).collect(Collectors.toList());
    }

    public Map<String, Integer> getSameNameMethodCounts() {
        if (sameNameMethodCounts.isEmpty()) {
            for (ServiceMethod method : methods) {
                sameNameMethodCounts.merge(method.name, 1, Integer::sum);
            }
        }
        return sameNameMethodCounts;
    }

    public String getSuperName() {
        return superName;
    }

    public void setSuperName(String superName) {
        this.superName = superName;
    }

    public String getSuperTypeParameters() {
        return superTypeParameters;
    }

    public void setSuperTypeParameters(String superTypeParameters) {
        this.superTypeParameters = superTypeParameters;
    }

    public String getSuperProxyName() {
        String superProxyName;

        if (superName.equals(Service.class.getName())) {
            superProxyName = Proxy.class.getSimpleName();
        } else {
            superProxyName = superName + "Proxy";
            if (superTypeParameters != null) {
                superProxyName += superTypeParameters;
            }
        }

        return simplifyClassName(superProxyName);
    }

    public String getSuperCallerName() {
        String superCallerName;
        if (superName.equals(Service.class.getName())) {
            superCallerName = Caller.class.getSimpleName();
        } else {
            superCallerName = superName + "Caller";
        }

        return simplifyClassName(superCallerName);
    }

    public String getIdType() {
        return idType;
    }

    public void setIdType(String idType) {
        this.idType = idType;
    }

    public boolean isCustomProxyPath() {
        return customProxyPath;
    }

    public void setCustomProxyPath(boolean customProxyPath) {
        this.customProxyPath = customProxyPath;
    }


    public Set<Integer> getProxyConstructors() {
        return proxyConstructors;
    }

    public void setProxyConstructors(Set<Integer> proxyConstructors) {
        this.proxyConstructors = proxyConstructors;
    }

    public boolean hasConstructor(int c) {
        return !_abstract && (proxyConstructors.isEmpty() || proxyConstructors.contains(c));
    }

    /**
     * 构造对象时使用的默认泛型参数列表字符串，例如:<?,?>
     */
    public String getTypeParametersStr2() {
        StringBuilder sb = new StringBuilder();

        if (!typeParameterBounds.isEmpty()) {
            sb.append("<");
            for (int i = 0; i < typeParameterBounds.size(); i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append("?");
            }
            sb.append(">");
        }

        return sb.toString();
    }

    @Override
    public void prepare() {
        super.prepare();
        idType = simplifyClassName(idType);
        superTypeParameters = simplifyClassName(superTypeParameters);
        methods.forEach(ServiceMethod::prepare);
    }

    @Override
    public String toString() {
        return "ServiceClass{" +
                "name='" + name + '\'' +
                "idType='" + idType + '\'' +
                ", packageName='" + packageName + '\'' +
                ", typeParameters=" + typeParametersStr +
                ", comment='" + comment + '\'' +
                ", methods=" + methods +
                '}';
    }

}
