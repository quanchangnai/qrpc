package quan.rpc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ServiceClassDefinition extends ServiceDefinition {

    private final String fullName;

    private String packageName;

    //是不是抽象类
    private boolean _abstract;

    private final List<ServiceMethodDefinition> methods = new ArrayList<>();

    //同名方法的数量
    private final Map<String, List<ServiceMethodDefinition>> sameNameMethods = new HashMap<>();

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

    //代理类和方法是否链接到服务类和方法
    private boolean proxyLinkToService;

    public ServiceClassDefinition(String fullName) {
        this.fullName = fullName;
        int index = fullName.lastIndexOf(".");
        if (index > 0) {
            this.packageName = fullName.substring(0, index);
            this.name = fullName.substring(index + 1);
        } else {
            this.name = fullName;
        }
        this.serviceClassDefinition = this;
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

    public List<ServiceMethodDefinition> getMethods() {
        return methods;
    }

    public List<ServiceMethodDefinition> getMethods(boolean hasExpiredTime) {
        return methods.stream().filter(m -> hasExpiredTime == (m.getExpiredTime() > 0)).collect(Collectors.toList());
    }

    public Map<String, List<ServiceMethodDefinition>> getSameNameMethods() {
        if (sameNameMethods.isEmpty()) {
            for (ServiceMethodDefinition method : methods) {
                sameNameMethods.computeIfAbsent(method.name, k -> new ArrayList<>()).add(method);
            }
        }
        return sameNameMethods;
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

    public String getSuperInvokerName() {
        String superInvokerName;
        if (superName.equals(Service.class.getName())) {
            superInvokerName = Invoker.class.getSimpleName();
        } else {
            superInvokerName = superName + "Invoker";
        }

        return simplifyClassName(superInvokerName);
    }

    public String getIdType() {
        return idType;
    }

    public void setIdType(String idType) {
        this.idType = idType;
    }

    public boolean isProxyLinkToService() {
        return proxyLinkToService;
    }

    public void setProxyLinkToService(boolean proxyLinkToService) {
        this.proxyLinkToService = proxyLinkToService;
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
        methods.forEach(ServiceMethodDefinition::prepare);
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
