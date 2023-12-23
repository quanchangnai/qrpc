package quan.rpc;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

public class ServiceMethod extends ServiceElement {

    private int security;

    public String returnType;

    //参数名:参数类型
    private final HashMap<String, String> parameters = new LinkedHashMap<>();

    private int expiredTime;

    public ServiceMethod(CharSequence name) {
        this.name = name.toString();
    }

    public void setSafeArgs(boolean safeArgs) {
        if (safeArgs) {
            security |= 0b01;
        }
    }

    public void setSafeReturn(boolean safeReturn) {
        if (safeReturn) {
            security |= 0b10;
        }
    }

    public int getSecurity() {
        return security;
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

    public void setServiceClass(ServiceClass serviceClass) {
        this.serviceClass = serviceClass;
    }

    public HashMap<String, String> getParameters() {
        return parameters;
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
        return typeParameterBounds.containsKey(parameterType) || serviceClass.typeParameterBounds.containsKey(parameterType);
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
            typeBounds = serviceClass.typeParameterBounds.get(parameterType);
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
        StringBuilder signature = new StringBuilder();
        signature.append(name);

        if (serviceClass.getSameNameMethodCounts().get(name) == 1) {
            return signature.toString();
        }

        signature.append("(");

        int i = 0;
        for (String parameterType : parameters.values()) {
            if (i++ > 0) {
                signature.append(", ");
            }
            int index = parameterType.indexOf("<");
            if (index > 0) {
                //删掉泛型后缀
                parameterType = parameterType.substring(0, index);
            }
            signature.append(simplifyClassName(parameterType));
        }

        signature.append(")");

        return signature.toString();
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
        return "ServiceMethod{" +
                "name='" + name + '\'' +
                ", comment='" + comment + '\'' +
                ", typeParameters=" + typeParametersStr +
                ", returnType='" + returnType + '\'' +
                ", parameters=" + parameters +
                '}';
    }

}
