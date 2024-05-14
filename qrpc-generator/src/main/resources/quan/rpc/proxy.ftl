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
 *<#if proxyLinkToService> @see</#if> ${name}
 */
public<#if abstract> abstract</#if> class ${name}Proxy${typeParametersStr} extends ${superProxyName} {

    public static final String SERVICE_NAME = "${fullName}";

    private static final String[] methodLabels = {
        <#list methods as method>
            SERVICE_NAME + ".${method.label}"<#if method?has_next>,</#if>
        </#list>
    };

    <#if hasConstructor(1)>
        <#if hasTypeParameters()>
    /**
     * 无参服务代理单例
     */
    public static final ${name}Proxy${typeParametersStr2} instance = new ${name}Proxy<>();
        <#else>
    public static final ${name}Proxy instance = new ${name}Proxy();
        </#if>

    /**
     * @see ProxyConstructors#NO_ARGS
     */
    public ${name}Proxy() {
    }

    <#else>
    protected ${name}Proxy() {
    }

    </#if>
    <#if hasConstructor(2)>
    /**
     * @see ProxyConstructors#NODE_ID
     */
    public ${name}Proxy(int nodeId) {
        setNodeId$(nodeId);
    }

    </#if>
    <#if hasConstructor(3)>
    /**
     * @see ProxyConstructors#SERVICE_ID
     */
    public ${name}Proxy(${idType} serviceId) {
        setServiceId$(serviceId);
    }

    </#if>
    <#if hasConstructor(4)>
    /**
     * @see ProxyConstructors#NODE_ID_AND_SERVICE_ID
     */
    public ${name}Proxy(int nodeId, ${idType} serviceId) {
        setNodeId$(nodeId);
        setServiceId$(serviceId);
    }

    </#if>
    <#if hasConstructor(5)>
    /**
     * @see ProxyConstructors#NODE_ID_RESOLVER
     */
    public ${name}Proxy(NodeIdResolver nodeIdResolver) {
        setNodeIdResolver$(nodeIdResolver);
    }

    </#if>
    <#if hasConstructor(6)>
    /**
     * @see ProxyConstructors#NODE_ID_RESOLVER_AND_SERVICE_ID
     */
    public ${name}Proxy(NodeIdResolver nodeIdResolver, ${idType} serviceId) {
        setNodeIdResolver$(nodeIdResolver);
        setServiceId$(serviceId);
    }

    </#if>
    <#if hasConstructor(7) && idType!='Object'>
    /**
     * @see ProxyConstructors#SHARDING_KEY
     */
    public ${name}Proxy(Object shardingKey) {
        setShardingKey$(shardingKey);
    }

    </#if>
    @Override
    protected String getServiceName$() {
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
     *<#if proxyLinkToService> @see</#if> ${name}#${method.signature}
     */
    public <#if method.typeParametersStr!="">${method.typeParametersStr} </#if>Promise<${method.returnType}> ${method.name}(<#rt>
    <#list method.parameters?keys as paramName>
        ${method.parameters[paramName]} ${paramName}<#if paramName?has_next>, </#if><#t>
    </#list>
    <#lt>) {
        return sendRequest$(${method.id}, methodLabels[${method?index}], ${method.security}, ${method.expiredTime}<#rt>
        <#lt><#if method.oneArrayParam>, (Object) ${method.parameters?keys?first}<#elseif method.parameters?keys?size gt 0>, ${method.parameters?keys?join(', ')}</#if>);
    }

</#list>
}
