<#if packageName??>
package ${packageName};

</#if>
import java.util.*;
import quan.rpc.*;

/**
 * @see ${name}
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public final class ${name}Invoker extends Invoker {

    public static final ${name}Invoker instance = new ${name}Invoker();

    private ${name}Invoker() {
    }

    @Override
    public Object invoke(Service service, int methodId, Object... params) throws Throwable {
        ${name} ${name?uncap_first} = (${name}) service;
        
        switch (methodId) {
        <#list methods as method>
            case ${method.id}:
            <#if method.returnVoid>
                ${name?uncap_first}.${method.name}(<#rt>
            <#else>
                return ${name?uncap_first}.${method.name}(<#rt>
            </#if>
            <#list method.parameters?keys as paramName>
                <#if method.isGenericTypeVar(paramName)>
                params[${paramName?index}]<#t>
                <#else>
                (${method.getAssignedType(paramName)}) params[${paramName?index}]<#t>
                </#if>
                <#if paramName?has_next>, </#if><#t>
            </#list>
            <#lt>);
            <#if method.returnVoid>
                return null;
            </#if>
        </#list>
            default:
                return ${superInvokerName}.instance.invoke(service, methodId, params);
        }
    }

    @Override
    public String getMethodLabel(int methodId) {
        switch (methodId) {
        <#list methods as method>
            case ${method.id}:
                return "${fullName}.${method.label}";
        </#list>
            default:
                return ${superInvokerName}.instance.getMethodLabel(methodId);
        }
    }

    @Override
    public int getExpiredTime(int methodId) {
        switch (methodId) {
        <#list getMethods(false) as method>
            case ${method.id}:
            <#if !method?has_next>
                return 0;
            </#if>
        </#list>
        <#list getMethods(true) as method>
            case ${method.id}:
                return ${method.expiredTime};
        </#list>
            default:
                return ${superInvokerName}.instance.getExpiredTime(methodId);
        }
    }

}
