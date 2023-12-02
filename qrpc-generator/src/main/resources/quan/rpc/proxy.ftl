<#if packageName??>
package ${packageName};

</#if>
import java.util.*;
import quan.rpc.*;

/**
<#list comments as comment>
 *${comment}
</#list>
<#if comments?size gt 0>
 *
</#if>
 *<#if !customProxyPath> @see</#if> ${name}
 */
public<#if abstract> abstract</#if> class ${name}Proxy${typeParametersStr} extends ${superProxyName} {

    public static final String SERVICE_NAME = "${fullName}";

    private static final String[] signatures = {
        <#list methods as method>
            SERVICE_NAME + ".${method.signature?replace(' ','')}"<#if method?has_next>,</#if>
        </#list>
    };

    <#if hasConstructor(1)>
        <#if hasTypeParameters()>
    public static final ${name}Proxy${typeParametersStr2} instance = new ${name}Proxy<>();
        <#else>
    public static final ${name}Proxy instance = new ${name}Proxy();
        </#if>

    public ${name}Proxy() {
    }

    </#if>
    <#if hasConstructor(2)>
    public ${name}Proxy(int nodeId) {
        _setNodeId$(nodeId);
    }

    </#if>
    <#if hasConstructor(3)>
    public ${name}Proxy(${idType} serviceId) {
        _setServiceId$(serviceId);
    }

    </#if>
    <#if hasConstructor(4)>
    public ${name}Proxy(int nodeId, ${idType} serviceId) {
        _setNodeId$(nodeId);
        _setServiceId$(serviceId);
    }

    </#if>
    <#if hasConstructor(5)>
    public ${name}Proxy(NodeIdResolver nodeIdResolver) {
        _setNodeIdResolver$(nodeIdResolver);
    }

    </#if>
    <#if hasConstructor(6)>
    public ${name}Proxy(NodeIdResolver nodeIdResolver, ${idType} serviceId) {
        _setNodeIdResolver$(nodeIdResolver);
        _setServiceId$(serviceId);
    }

    </#if>
    /**
     * 对应的服务名
     */
    @Override
    protected String _getServiceName$() {
        return SERVICE_NAME;
    }

<#list methods as method>
    /**
    <#list method.comments as comment>
     *${comment}
    </#list>
    <#if method.comments?size gt 0>
     *
    </#if>
     *<#if !customProxyPath> @see</#if> ${name}#${method.signature}
     */
    public <#if method.typeParametersStr!="">${method.typeParametersStr} </#if>Promise<${method.returnType}> ${method.name}(<#rt>
    <#list method.parameters?keys as paramName>
        ${method.parameters[paramName]} ${paramName}<#if paramName?has_next>, </#if><#t>
    </#list>
    <#lt>) {
        return _sendRequest$(${method.id}, signatures[${method?index}], ${method.securityModifier}<#rt>
        <#lt><#if method.oneArrayParam>, (Object) ${method.parameters?keys?first}<#elseif method.parameters?keys?size gt 0>, ${method.parameters?keys?join(', ')}</#if>);
    }

</#list>
}
