package quan.rpc;

import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

public class ServiceMethodDefinition extends ServiceDefinition {

    /**
     * 参数名:参数类型
     */
    private final HashMap<String, String> parameters = new LinkedHashMap<>();

    private String returnType;

    private boolean safeArgs;

    private boolean safeReturn;

    /**
     * 所在源文件以及行号
     */
    private String sourceLine;

    private int expiredTime;

    public ServiceMethodDefinition(CharSequence name) {
        this.name = name.toString();
    }

    public void setSafeArgs(boolean safeArgs) {
        this.safeArgs = safeArgs;
    }

    public void setSafeReturn(boolean safeReturn) {
        this.safeReturn = safeReturn;
    }

    public void setServiceClass(ServiceClassDefinition serviceClassDefinition) {
        this.serviceClassDefinition = serviceClassDefinition;
    }


    public String getReturnType() {
        return returnType;
    }

    public void setReturnType(String returnType) {
        this.returnType = returnType;
    }

    public boolean isReturnVoid() {
        return Void.class.getSimpleName().equals(returnType);
    }

    public void addParameter(CharSequence name, String type) {
        parameters.put(name.toString(), type);
    }

    public HashMap<String, String> getParameters() {
        return parameters;
    }

    public boolean isSafeArgs() {
        return safeArgs;
    }

    public boolean isSafeReturn() {
        return safeReturn;
    }

    public void setSourceLine(String sourceLine) {
        this.sourceLine = sourceLine;
    }

    public int getExpiredTime() {
        return expiredTime;
    }

    public void setExpiredTime(int expiredTime) {
        this.expiredTime = expiredTime;
    }

    /**
     * 返回数组类型参数的组件类型
     */
    public String getArrayComponentType(String parameterName) {
        String componentType;
        String parameterType = parameters.get(parameterName);

        if (parameterType.endsWith("[]")) {
            componentType = parameterType.substring(0, parameterType.length() - 2);
        } else if (parameterType.endsWith("...")) {
            componentType = parameterType.substring(0, parameterType.length() - 3);
        } else {
            componentType = parameterType;
        }

        return simplifyClassName(componentType);
    }

    /**
     * 判断方法只有一个数组参数，可变参数也算一个
     */
    public boolean isOneArrayParam() {
        if (parameters.size() == 1) {
            String parameterType = parameters.values().stream().findFirst().orElse("");
            return parameterType.endsWith("[]") || parameterType.endsWith("...");
        } else {
            return false;
        }
    }


    /**
     * 判断参数是不是泛型变量
     */
    public boolean isGenericTypeVar(String parameterName) {
        String parameterType = parameters.get(parameterName);
        return typeParameterBounds.containsKey(parameterType) || serviceClassDefinition.typeParameterBounds.containsKey(parameterType);
    }

    /**
     * 返回参数接收赋值的类型
     */
    public String getAssignedType(String parameterName) {
        String parameterType = parameters.get(parameterName);
        if (parameterType.endsWith("...")) {
            parameterType = parameterType.substring(0, parameterType.length() - 3) + "[]";
        }

        int index = parameterType.indexOf("<");
        if (index > 0) {
            //擦除泛型
            return parameterType.substring(0, index);
        }

        //泛型变量上界
        List<String> typeBounds = typeParameterBounds.get(parameterType);
        if (typeBounds == null || typeBounds.isEmpty()) {
            typeBounds = serviceClassDefinition.typeParameterBounds.get(parameterType);
        }

        if (typeBounds != null && !typeBounds.isEmpty()) {
            if (typeBounds.contains(Object.class.getName()) && typeBounds.size() > 1) {
                return typeBounds.get(1);
            } else {
                return typeBounds.get(0);
            }
        }

        return parameterType;
    }

    public String getSignature() {
        StringBuilder sb = new StringBuilder();
        sb.append(name);

        if (serviceClassDefinition.getSameNameMethods().get(name).size() == 1) {
            return sb.toString();
        }

        sb.append("(");

        int i = 0;
        for (String parameterType : parameters.values()) {
            if (i++ > 0) {
                sb.append(", ");
            }
            int index = parameterType.indexOf("<");
            if (index > 0) {
                //删掉泛型后缀
                parameterType = parameterType.substring(0, index);
            }
            sb.append(simplifyClassName(parameterType));
        }

        sb.append(")");

        return sb.toString();
    }

    public String getLabel() {
        if (StringUtils.isBlank(sourceLine)) {
            List<ServiceMethodDefinition> sameNameMethods = serviceClassDefinition.getSameNameMethods().get(name);
            if (sameNameMethods.size() == 1) {
                return name;
            } else {
                return name + ":" + (sameNameMethods.indexOf(this) + 1);
            }
        } else {
            return name + "(" + sourceLine + ")";
        }
    }

    @Override
    public void prepare() {
        super.prepare();
        returnType = simplifyClassName(returnType);

        HashMap<String, String> _parameters = new LinkedHashMap<>();

        for (String name : this.parameters.keySet()) {
            String type = simplifyClassName(this.parameters.get(name));
            _parameters.put(name, type);
        }

        this.parameters.putAll(_parameters);

    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                "name='" + name + '\'' +
                ", typeParameters=" + typeParametersStr +
                ", returnType='" + returnType + '\'' +
                ", parameters=" + parameters +
                ", safeArgs='" + safeArgs + '\'' +
                ", safeReturn='" + safeReturn + '\'' +
                ", sourceLine='" + sourceLine + '\'' +
                ", comment='" + comment + '\'' +
                '}';
    }

}
