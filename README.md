# QRPC 框架技术文档

## 目录

- [1. 项目概述](#1-项目概述)
- [2. 项目结构](#2-项目结构)
- [3. 核心架构](#3-核心架构)
- [4. 代码生成机制](#4-代码生成机制)
- [5. 线程模型](#5-线程模型)
- [6. 定时器系统](#6-定时器系统)
- [7. 高级特性](#7-高级特性)
- [8. 使用示例](#8-使用示例)
- [9. 最佳实践](#9-最佳实践)
- [10. 常见问题](#10-常见问题)
- [11. 扩展开发](#11-扩展开发)
- [12. 版本历史](#12-版本历史)
- [13. 参考资料](#13-参考资料)
- [附录 A: 完整 API 参考](#附录-a-完整-api-参考)

---

## 1. 项目概述

### 1.1 项目简介
QRPC (Quan RPC) 是一个轻量级、高性能的 Java RPC 框架，支持异步调用、多种传输协议和灵活的线程模型。该框架采用注解处理器自动生成代理类和调用器，简化了远程服务调用的开发体验。

### 1.2 核心特性
- **纯异步调用**：所有远程调用均返回 `Promise`，强制异步编程模型
- **代码生成**：编译时自动生成 Proxy 和 Invoker 类，零反射开销
- **多传输协议**：支持 Netty TCP 和 RabbitMQ 两种通信方式
- **灵活线程模型**：单线程工作者 + 线程池工作者的混合模式
- **服务分片**：支持基于分片键的服务路由
- **超时控制**：完善的调用超时和过期机制
- **定时器支持**：内置定时器和 Cron 表达式支持

### 1.3 技术栈
- **Java 8+**
- **Netty 4.1.108** - 高性能网络通信
- **RabbitMQ 5.20.0** - 消息队列通信
- **Protostuff 1.8.0** - 序列化框架
- **FreeMarker 2.3.30** - 代码生成模板引擎
- **SLF4J + Log4j2** - 日志框架
- **Cron Utils 9.1.8** - Cron 表达式解析

---

## 2. 项目结构

```
qrpc/
├── qrpc-runtime/          # 运行时核心模块
│   └── src/main/java/quan/rpc/
│       ├── Node.java              # RPC 节点
│       ├── Worker.java            # 单线程工作者
│       ├── ThreadPoolWorker.java  # 线程池工作者
│       ├── Service.java           # 服务基类
│       ├── Proxy.java             # 代理基类
│       ├── Invoker.java           # 调用器基类
│       ├── Promise.java           # 异步结果封装
│       ├── Connector.java         # 连接器抽象
│       ├── NettyConnector.java    # Netty 连接器
│       ├── RabbitConnector.java   # RabbitMQ 连接器
│       ├── Protocol.java          # 通信协议
│       ├── Endpoint.java          # 端点注解
│       ├── ProxyConstructors.java # 代理构造器注解
│       ├── Timer/TimerMgr.java    # 定时器管理
│       └── ...
│
├── qrpc-generator/        # 代码生成模块
│   └── src/main/java/quan/rpc/
│       ├── Generator.java         # 注解处理器
│       ├── ServiceClassDefinition.java    # 服务类定义
│       ├── ServiceMethodDefinition.java   # 服务方法定义
│       └── resources/
│           ├── proxy.ftl          # Proxy 模板
│           └── invoker.ftl        # Invoker 模板
│
└── build.gradle           # Gradle 构建配置
```

---

## 3. 核心架构

### 3.1 整体架构图

```
┌─────────────────────────────────────────────────────┐
│                      Node (RPC节点)                   │
│                                                       │
│  ┌──────────┐  ┌──────────┐  ┌──────────────────┐  │
│  │ Worker 1 │  │ Worker 2 │  │ ThreadPoolWorker │  │
│  │(单线程)   │  │(单线程)   │  │  (线程池)         │  │
│  │          │  │          │  │                  │  │
│  │ Services │  │ Services │  │   Services       │  │
│  └──────────┘  └──────────┘  └──────────────────┘  │
│                                                       │
│  ┌──────────────────────────────────────────────┐   │
│  │          Connectors (连接器层)                 │   │
│  │  ┌──────────────┐  ┌──────────────────┐     │   │
│  │  │ NettyConn    │  │ RabbitMQConn     │     │   │
│  │  └──────────────┘  └──────────────────┘     │   │
│  └──────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────┘
         ↓                    ↓
    Remote Node 1      Remote Node 2
```

### 3.2 核心组件说明

#### 3.2.1 Node (RPC 节点)

**职责**：代表一个 RPC 服务节点，管理所有工作者和服务实例

**关键功能**：
- 管理工作者集合（单线程 + 线程池）
- 管理服务注册与发现
- 处理请求和响应的路由
- 定期执行刷帧任务（update）

**主要方法**：
```java
// 创建节点
Node node = new Node(nodeId, config, connector);

// 注册服务
node.addService(service);
node.addService(service, worker);
node.addService(service, predicate); // 根据条件选择worker

// 启动/停止
node.start();
node.stop();
```

**配置项 (Node.Config)**：
```java
Config config = new Config();
config.setSingleThreadWorkerNum(4);           // 单线程工作者数量
config.setMaxUpdateWaitTime(200);             // 最大刷帧等待时间(ms)
config.setMaxUpdateCostTime(200);             // 最大刷帧消耗时间(ms)
config.setCallTtl(10);                        // 默认调用超时(秒)
config.setMaxCallTtl(300);                    // 最大调用超时(秒)
config.setNodeIdResolver(resolver);           // 节点ID解析器
config.setServiceIdResolver(resolver);        // 服务ID解析器
config.addThreadPoolWorker(10, 300);          // 添加线程池工作者
```

#### 3.2.2 Worker (工作者)

**职责**：执行服务方法的线程单元，保证同一服务的并发安全

**两种类型**：
1. **Worker (单线程)**：每个 Worker 独占一个线程，适合有状态服务
2. **ThreadPoolWorker (线程池)**：使用线程池执行，适合无状态高并发服务

**关键功能**：
- 管理分配给该 Worker 的服务实例
- 维护待处理的 Promise（异步调用）
- 执行定时任务更新
- 处理请求和响应

**线程模型**：
```
Worker ID 编码规则：
callId = (workerId << 32) | sequenceNumber

这样可以从 callId 快速定位到对应的 Worker
```

#### 3.2.3 Service (服务)

**职责**：提供远程可调用的业务逻辑

**使用方式**：
```java
@ProxyConstructors({NO_ARGS, NODE_ID, SERVICE_ID})
public abstract class MyService extends Service<Long> {
    
    @Override
    public Long getId() {
        return 1L; // 服务ID，在Node内唯一
    }
    
    @Endpoint
    public String hello(String name) {
        return "Hello, " + name;
    }
    
    @Endpoint(safeArgs = true, safeReturn = true)
    public int add(int a, int b) {
        return a + b;
    }
    
    @Endpoint(expiredTime = 30) // 30秒超时
    public Promise<Integer> asyncTask() {
        DelayedResult<Integer> result = newDelayedResult();
        execute(() -> {
            // 异步执行
            result.setResult(42);
        });
        return result;
    }
}
```

**生命周期**：
```
创建 → init() → 运行中 → destroy() → 销毁
         ↑                ↑
      Worker启动      Worker停止
```

#### 3.2.4 Proxy (代理) & Invoker (调用器)

**职责**：
- **Proxy**：客户端侧，将方法调用转换为 RPC 请求
- **Invoker**：服务端侧，将 RPC 请求转换为方法调用

**代码生成**：编译时由 `Generator` 注解处理器自动生成，无需手写。

**Proxy 示例**（自动生成）：
```java
public class TestService1Proxy extends Proxy {
    
    public static final String SERVICE_NAME = "quan.rpc.test.TestService1";
    
    private static final MethodInfo[] SERVICE_METHODS = {
        new MethodInfo(1, "TestService1.java:132", false, false, 0),
        new MethodInfo(2, "TestService1.java:146", true, true, 0),
        // ...
    };
    
    // 多种构造方式
    public TestService1Proxy() { }
    public TestService1Proxy(int nodeId) { setNodeId$(nodeId); }
    public TestService1Proxy(Object serviceId) { setServiceId$(serviceId); }
    public TestService1Proxy(int nodeId, Object serviceId) { ... }
    public TestService1Proxy(NodeIdResolver resolver, Object serviceId) { ... }
    
    // 所有方法都是异步的，返回 Promise
    public Promise<Integer> add1(Integer a, Integer b) {
        return sendRequest$(SERVICE_METHODS[0], a, b);
    }
    
    public Promise<Integer> add2(Integer a, Integer b) {
        return sendRequest$(SERVICE_METHODS[1], a, b);
    }
}
```

**Invoker 示例**（自动生成）：
```java
public class TestService1Invoker extends Invoker {
    
    public static final TestService1Invoker instance = new TestService1Invoker();
    
    @Override
    public Object invoke(Service<?> service, int methodId, Object... params) {
        TestService1 s = (TestService1) service;
        switch (methodId) {
            case 1: return s.add1((Integer)params[0], (Integer)params[1]);
            case 2: return s.add2((Integer)params[0], (Integer)params[1]);
            // ...
        }
    }
}
```

#### 3.2.5 Promise (异步结果)

**职责**：封装异步调用的结果，提供链式处理能力

**设计思路**：类似 CompletableFuture，但针对 RPC 场景优化

**使用示例**：
```java
// 基本用法
Promise<Integer> promise = serviceProxy.add1(10, 20);

// 链式处理
promise
    .thenAccept(result -> {
        System.out.println("结果: " + result);
    })
    .exceptionally(e -> {
        System.err.println("异常: " + e.getMessage());
    })
    .completely(() -> {
        System.out.println("最终清理");
    });

// 组合多个Promise
Promise<Integer> p1 = serviceProxy.add1(1, 2);
Promise<Integer> p2 = serviceProxy.add1(3, 4);

Promise.allOf(p1, p2).thenRun(() -> {
    System.out.println("两个调用都完成了");
});

// thenCompose 链式调用
serviceProxy.login1(a, b)
    .thenCompose(result1 -> serviceProxy.login2(a, b + 1))
    .thenCompose(result2 -> serviceProxy.login3(a, b + 2))
    .thenAccept(result3 -> {
        System.out.println("最终结果: " + result3);
    });
```

**状态流转**：
```
PENDING → OK (成功) 或 FAILED (失败/超时)
```

**超时机制**：
- 每个 Promise 有过期时间戳
- Worker 定期扫描过期的 Promise 并标记为超时
- 超时触发 CallException.TIME_OUT

#### 3.2.6 Connector (连接器)

**职责**：负责节点间的网络通信

**支持的类型**：

1. **NettyConnector** - 基于 TCP 的直接连接
```java
NettyConnector connector = new NettyConnector("127.0.0.1", 8888);
connector.addRemote(1, "127.0.0.1", 9999); // 添加远程节点
```

2. **RabbitConnector** - 基于 RabbitMQ 的消息队列
```java
ConnectionFactory factory = new ConnectionFactory();
factory.setHost("127.0.0.1");
RabbitConnector connector = new RabbitConnector(factory);
```

**通信协议 (Protocol)**：
```java
// 请求协议
Request {
    originNodeId: int      // 来源节点ID
    callId: long           // 调用ID
    serviceId: Object      // 目标服务ID
    methodId: int          // 目标方法ID
    params: Object[]       // 方法参数
}

// 响应协议
Response {
    originNodeId: int      // 来源节点ID
    callId: long           // 调用ID
    result: Object         // 返回结果
    exception: Object      // 异常信息
}

// 握手协议（Netty专用）
Handshake {
    originNodeId: int
    ip: String
    port: int
}

// 心跳协议
PingPong {
    originNodeId: int
    time: long
}
```

---

## 4. 代码生成机制

### 4.1 注解处理器 (Generator)

**工作原理**：
1. 编译时扫描 `@Endpoint` 和 `@ProxyConstructors` 注解
2. 解析服务类和方法的元数据
3. 使用 FreeMarker 模板生成 Proxy 和 Invoker 类
4. 生成的类与服务类在同一包下

**配置选项**：
```gradle
compileJava {
    options.compilerArgs += [
        "-ArpcProxyPath=${projectDir}/src/main/java",  // 自定义生成路径
        "-ArpcProxyLinkToService=true"                  // 生成 @see 链接
    ]
}
```

### 4.2 注解说明

#### @Endpoint

标记可远程调用的方法

```java
@Endpoint(
    safeArgs = false,      // 参数是否安全（不可变）
    safeReturn = true,     // 返回值是否安全
    expiredTime = 0        // 超时时间(秒)，0表示使用默认值
)
public String myMethod(String param) { ... }
```

**安全性说明**：
- `safeArgs/safeReturn = true`：不进行序列化克隆，性能更好
- `safeArgs/safeReturn = false`：跨节点调用时会克隆对象，避免共享引用
- 原生类型及其包装类自动视为安全

#### @ProxyConstructors

指定生成的 Proxy 类支持哪些构造方式

```java
@ProxyConstructors({
    NO_ARGS,                          // 无参构造
    NODE_ID,                          // 指定节点ID
    SERVICE_ID,                       // 指定服务ID
    NODE_ID_AND_SERVICE_ID,           // 同时指定
    NODE_ID_RESOLVER,                 // 使用节点ID解析器
    NODE_ID_RESOLVER_AND_SERVICE_ID,  // 解析器+服务ID
    SHARDING_KEY                      // 使用分片键
})
public abstract class MyService extends Service<Long> { ... }
```

### 4.3 方法 ID 分配规则

- 从父类到子类依次分配
- 每个 `@Endpoint` 方法获得唯一 ID
- 继承体系中方法 ID 连续递增

```
Service (基类)
  └─ Method A (id=1)
  
MyService extends Service
  ├─ Method B (id=2)
  └─ Method C (id=3)
```

---

## 5. 线程模型

### 5.1 工作者类型

#### 单线程工作者 (Worker)

**特点**：
- 每个 Worker 独占一个线程
- 分配给该 Worker 的所有服务在该线程上串行执行
- 适合有状态服务，无需同步锁
- 默认数量 = CPU 核心数

#### 线程池工作者 (ThreadPoolWorker)

**特点**：
- 使用线程池并行执行
- 适合无状态、高并发服务
- 需要自行处理线程安全问题
- 可配置核心线程数和最大线程数

### 5.2 服务分配到 Worker 的策略

```java
// 1. 随机分配
node.addService(service);

// 2. 指定 Worker ID
node.addService(service, workerId);

// 3. 指定 Worker 实例
node.addService(service, worker);

// 4. 根据条件选择（如只分配到线程池）
node.addService(service, Worker::isThreadPool);
```

### 5.3 刷帧机制 (Update Loop)

**概念**：定期执行的维护任务，类似游戏服务器的 tick

**执行内容**：
1. 更新定时器（TimerMgr）
2. 检查并移除过期的 Promise
3. 监控性能指标

**配置**：
```java
config.setUpdateInterval(50);              // 刷帧间隔 50ms
config.setMaxUpdateWaitTime(200);          // 最大等待时间
config.setMaxUpdateCostTime(200);          // 最大执行时间
config.setMaxUpdateInterval(1000);         // 最大间隔报警阈值
```

**性能监控**：
- 等待时间过长 → 可能任务队列积压
- 执行时间过长 → 可能有耗时任务阻塞
- 间隔时间过长 → 线程可能卡死

---

## 6. 定时器系统

### 6.1 Timer 类型

```java
// 1. 延迟执行（一次性）
newTimer(() -> {
    System.out.println("延迟1秒执行");
}, 1000);

// 2. 周期性执行
newTimer(() -> {
    System.out.println("每5秒执行一次");
}, 1000, 5000); // 延迟1秒，周期5秒

// 3. Cron 表达式
newTimer(() -> {
    System.out.println("每10秒执行");
}, "0/10 * * * * ?");
```

### 6.2 注解方式

```java
@Timer.Delay(1000)
private void delayedTask() {
    // 延迟1秒执行
}

@Timer.Period(delay = 1000, period = 5000)
private void periodicTask() {
    // 周期性执行
}

@Timer.Cron("0/10 * * * * ?")
private void cronTask() {
    // Cron 调度
}
```

### 6.3 实现原理

- 每个 Worker 有自己的 TimerMgr
- 使用优先级队列按到期时间排序
- 刷帧时批量处理到期的定时器
- 支持在服务初始化时自动注册注解定时器

---

## 7. 高级特性

### 7.1 节点 ID 解析器 (NodeIdResolver)

**用途**：动态计算目标节点 ID

```java
NodeIdResolver resolver = new NodeIdResolver() {
    @Override
    public int resolveNodeId(Proxy proxy) {
        // 根据分片键或其他逻辑计算节点ID
        Object key = proxy.getShardingKey$();
        return calculateNodeId(key);
    }
    
    @Override
    public boolean isCacheNodeId(Proxy proxy) {
        return true; // 是否缓存计算结果
    }
};

// 使用
MyServiceProxy proxy = new MyServiceProxy(resolver);
```

### 7.2 服务 ID 解析器 (ServiceIdResolver)

**用途**：动态计算目标服务 ID

```java
ServiceIdResolver resolver = new ServiceIdResolver() {
    @Override
    public Object resolveServiceId(Proxy proxy) {
        // 根据代理实例计算服务ID
        return calculateServiceId(proxy);
    }
    
    @Override
    public boolean isCacheServiceId(Proxy proxy) {
        return false;
    }
};

// 配置到节点
config.setServiceIdResolver(resolver);
```

### 7.3 分片键路由

**用途**：基于业务键进行服务分片

```java
@ProxyConstructors({SHARDING_KEY})
public abstract class UserService extends Service<Long> {
    // ...
}

// 使用分片键
UserServiceProxy proxy = new UserServiceProxy(userId); // userId作为分片键
proxy.getUserName(); // 自动路由到正确的节点
```

### 7.4 DelayedResult (延迟结果)

**用途**：在异步场景中手动设置结果

```java
@Endpoint
public Promise<Integer> asyncCompute() {
    DelayedResult<Integer> result = newDelayedResult();
    
    // 在另一个线程或回调中设置结果
    execute(() -> {
        int value = heavyComputation();
        result.setResult(value);
        // 或者 result.setException(e);
    });
    
    return result;
}
```

### 7.5 异常传播

**配置**：
```java
config.setThrowExceptionToRemote(true); // 将异常对象传给远程节点
```

**行为**：
- `true`：传递完整异常对象（需要可序列化）
- `false`：只传递异常字符串（默认）

---

## 8. 使用示例

### 8.1 定义服务

```java
package com.example.service;

import quan.rpc.Endpoint;
import quan.rpc.Promise;
import quan.rpc.ProxyConstructors;
import quan.rpc.Service;

import static quan.rpc.ProxyConstructors.*;

@ProxyConstructors({NO_ARGS, NODE_ID, SERVICE_ID})
public abstract class OrderService extends Service<Long> {
    
    private Long orderId;
    
    public OrderService(Long orderId) {
        this.orderId = orderId;
    }
    
    @Override
    public Long getId() {
        return orderId;
    }
    
    @Endpoint
    public OrderInfo getOrderInfo() {
        // 查询订单
        return queryOrder(orderId);
    }
    
    @Endpoint(safeReturn = true)
    public int getStatus() {
        return orderStatus;
    }
    
    @Endpoint(expiredTime = 30)
    public Promise<Boolean> pay(BigDecimal amount) {
        DelayedResult<Boolean> result = newDelayedResult();
        
        // 异步支付处理
        paymentGateway.pay(amount, success -> {
            result.setResult(success);
        });
        
        return result;
    }
}
```

### 8.2 服务端启动

```java
package com.example.server;

import quan.rpc.Node;
import quan.rpc.NettyConnector;

public class OrderServer {
    public static void main(String[] args) {
        // 配置节点
        Node.Config config = new Node.Config();
        config.setSingleThreadWorkerNum(4);
        config.addThreadPoolWorker(10, 50);
        config.setCallTtl(10);
        
        // 创建连接器
        NettyConnector connector = new NettyConnector("127.0.0.1", 8888);
        
        // 创建节点
        Node node = new Node(1, config, connector);
        
        // 注册服务
        node.addService(new OrderService(1001L));
        node.addService(new OrderService(1002L));
        
        // 启动
        node.start();
        
        // 优雅关闭
        Runtime.getRuntime().addShutdownHook(new Thread(node::stop));
    }
}
```

### 8.3 客户端调用

**重要提示**：QRPC **仅支持异步调用**，所有远程方法都返回 `Promise<T>`，无法直接获取返回值。

```java
package com.example.client;

import quan.rpc.Node;
import quan.rpc.NettyConnector;
import com.example.service.OrderServiceProxy;

public class OrderClient {
    public static void main(String[] args) {
        // 配置节点
        Node.Config config = new Node.Config();
        NettyConnector connector = new NettyConnector("127.0.0.1", 9999);
        connector.addRemote(1, "127.0.0.1", 8888); // 添加远程节点
        
        Node node = new Node(2, config, connector);
        node.start();
        
        // 创建代理
        OrderServiceProxy proxy = new OrderServiceProxy(1, 1001L);
        
        // ⚠️ 注意：QRPC 只支持异步调用，所有方法返回 Promise
        
        // ✅ 正确方式1：使用 thenAccept 处理结果
        proxy.getOrderInfo()
            .thenAccept(info -> {
                System.out.println("订单信息: " + info);
            })
            .exceptionally(e -> {
                System.err.println("查询失败: " + e.getMessage());
                return null;
            });
        
        // ✅ 正确方式2：异步支付操作
        proxy.pay(new BigDecimal("99.99"))
            .thenAccept(success -> {
                if (success) {
                    System.out.println("支付成功");
                } else {
                    System.out.println("支付失败");
                }
            })
            .exceptionally(e -> {
                System.err.println("支付异常: " + e.getMessage());
                return null;
            });
        
        // ✅ 正确方式3：链式调用 - 转换和处理
        proxy.getStatus()
            .thenApply(status -> status == 1 ? "待支付" : "已完成")
            .thenAccept(desc -> System.out.println("状态: " + desc));
            
        // ❌ 错误示例：以下代码无法编译
        // OrderInfo info = proxy.getOrderInfo(); // 返回的是 Promise<OrderInfo>
        // int status = proxy.getStatus();         // 返回的是 Promise<Integer>
        
        // ⚠️ 不推荐：如果确实需要同步等待（会阻塞线程，失去异步优势）
        // 只能在 Worker 线程外使用
        /*
        try {
            OrderInfo info = proxy.getOrderInfo().future.get(5, TimeUnit.SECONDS);
            System.out.println("订单信息: " + info);
        } catch (Exception e) {
            e.printStackTrace();
        }
        */
    }
}
```

### 8.4 服务间调用

**注意**：服务间调用也是异步的，返回 Promise。

```java
public class OrderService extends Service<Long> {
    
    // 持有其他服务的代理
    private UserServiceProxy userServiceProxy = new UserServiceProxy();
    
    @Endpoint
    public Promise<OrderDetail> getOrderDetail() {
        OrderInfo order = getOrderInfo();
        
        // 远程调用用户服务（异步）
        Promise<UserInfo> userPromise = userServiceProxy.getUserInfo(order.getUserId());
        
        // 使用 thenCompose 链式处理
        return userPromise.thenApply(user -> {
            OrderDetail detail = new OrderDetail();
            detail.setOrder(order);
            detail.setUser(user);
            return detail;
        });
    }
}
```

---

## 9. 最佳实践

### 9.1 服务设计

1. **服务粒度**
   - 建议按业务领域划分服务
   - 单个服务不宜过大（方法数 < 50）
   - 避免循环依赖

2. **方法设计**
   - 优先使用异步返回（Promise）
   - 标记安全的参数和返回值以提升性能
   - 为耗时操作设置合理的超时时间

3. **服务 ID 设计**
   - 使用有意义的 ID（如业务主键）
   - 确保在 Node 内唯一
   - 考虑使用分片策略

```java
// ✅ 正确：单线程工作者中的服务无需同步
@ProxyConstructors({NODE_ID})
public class StatefulService extends Service<Long> {
    private int counter; // 无需volatile或synchronized
    
    @Endpoint
    public int increment() {
        return ++counter; // 线程安全
    }
}

// ❌ 错误：线程池工作者中需要自行保证线程安全
@ProxyConstructors({NODE_ID})
public class UnsafeService extends Service<Long> {
    private List<String> list = new ArrayList<>(); // 非线程安全
    
    @Endpoint
    public void add(String item) {
        list.add(item); // 需要ConcurrentHashMap或同步
    }
}
```

### 9.3 性能优化

1. **启用安全标记**

```java
@Endpoint(safeArgs = true, safeReturn = true)
public int add(int a, int b) {
    return a + b;
}
```

2. **合理设置超时**

```java
@Endpoint(expiredTime = 5) // 快速失败
public CacheData getCache(String key) {
    // ...
}
```

3. **批量操作**

```java
// 避免多次RPC
@Endpoint
public Promise<List<Order>> batchGetOrders(List<Long> ids) {
    // 一次性获取多个订单
}
```

4. **选择合适的 Worker 类型**

```java
// 有状态服务 → 单线程
node.addService(statefulService, Worker::isSingleThread);

// 无状态高并发 → 线程池
node.addService(statelessService, Worker::isThreadPool);
```

### 9.4 错误处理

```java
// 完整的错误处理
proxy.callRemote()
    .thenAccept(result -> {
        // 处理成功
    })
    .exceptionally(e -> {
        if (e instanceof CallException) {
            CallException ce = (CallException) e;
            switch (ce.getReason()) {
                case TIME_OUT:
                    // 超时处理
                    break;
                case DISCONNECTED:
                    // 断连处理
                    break;
                case REMOTE_ERROR:
                    // 远程异常
                    break;
            }
        }
        return null;
    })
    .completely(() -> {
        // 清理资源
    });
```

### 9.5 监控和日志

```java
// 启用性能监控
config.setMaxUpdateWaitTime(200);
config.setMaxUpdateCostTime(200);

// 日志配置（log4j2.xml）
<Logger name="quan.rpc" level="debug"/>

// 关键日志点：
// - 服务初始化/销毁
// - RPC 调用超时
// - 刷帧性能告警
// - 连接断开/重连
```

---

## 10. 常见问题

### Q1: 如何调试生成的 Proxy 和 Invoker？

**A**：配置生成路径到源码目录：

```gradle
compileJava {
    options.compilerArgs += ["-ArpcProxyPath=${projectDir}/src/main/java"]
}
```

生成的文件会出现在 `src/main/java` 对应包下，可以打断点调试。

### Q2: Promise 超时如何处理？

**A**：
```java
promise.exceptionally(e -> {
    if (e instanceof CallException && 
        ((CallException)e).getReason() == CallException.Reason.TIME_OUT) {
        // 超时处理逻辑
    }
    return defaultValue;
});
```

### Q3: 如何实现服务负载均衡？

**A**：使用 NodeIdResolver：

```java
NodeIdResolver resolver = new NodeIdResolver() {
    private AtomicInteger counter = new AtomicInteger(0);
    
    @Override
    public int resolveNodeId(Proxy proxy) {
        // 轮询策略
        int[] nodeIds = {1, 2, 3};
        return nodeIds[counter.getAndIncrement() % nodeIds.length];
    }
};
```

### Q4: 跨节点调用时对象共享问题？

**A**：不安全对象会自动克隆：

- 标记 `safeArgs/safeReturn = false`（默认）
- 跨节点调用时通过 Protostuff 序列化克隆
- 同节点调用不克隆（性能优化）

### Q5: 如何优雅关闭节点？

**A**：
```java
Runtime.getRuntime().addShutdownHook(new Thread(() -> {
    node.stop(); // 停止接受新请求
    // 等待正在处理的请求完成
    try {
        Thread.sleep(5000);
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
    }
}));
```

### Q6: 支持 Spring 集成吗？

**A**：当前版本不支持，但可以手动集成：

```java
@Component
public class ServiceInitializer {
    @Autowired
    private Node node;
    
    @PostConstruct
    public void init() {
        node.addService(new MyService());
    }
}
```

### Q7: 为什么不支持同步调用？

**A**: QRPC **强制使用异步编程模型**，原因如下：
- 避免线程阻塞，充分利用非阻塞 I/O 的优势
- 适合高并发、低延迟的实时应用场景（如游戏服务器、即时通讯等）
- 通过链式调用（thenCompose、thenApply）可以优雅地处理复杂的异步流程
- 所有远程方法都返回 `Promise<T>`，没有同步版本
- 如果确实需要同步等待，可以使用 `promise.future.get()`，但会失去异步优势且不推荐

---

## 11. 扩展开发

### 11.1 自定义 Connector

```java
public class CustomConnector extends Connector {
    
    @Override
    protected void start() {
        // 初始化连接
    }
    
    @Override
    protected void stop() {
        // 关闭连接
    }
    
    @Override
    protected boolean isLegalRemote(int remoteId) {
        // 判断是否是合法的远程节点
        return true;
    }
    
    @Override
    protected boolean isRemoteConnected(int remoteId) {
        // 判断连接状态
        return true;
    }
    
    @Override
    protected void sendProtocol(int remoteId, Protocol protocol) {
        // 发送协议
        byte[] data = encode(protocol);
        // 通过网络发送
    }
    
    // 接收数据时调用
    protected void onReceive(byte[] data) {
        Protocol protocol = decode(new ByteArrayInputStream(data));
        handleProtocol(protocol);
    }
}
```

---

## 12. 版本历史

### v1.0 (当前版本)
- ✅ 基础 RPC 功能
- ✅ 异步调用支持
- ✅ Netty 和 RabbitMQ 传输
- ✅ 代码生成
- ✅ 定时器系统
- ✅ 单线程和线程池工作者
- ✅ 超时和过期机制

## 13. 参考资料

### 相关项目
- [Netty](https://netty.io/)
- [RabbitMQ](https://www.rabbitmq.com/)
- [Protostuff](https://protostuff.github.io/)
- [CompletableFuture](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CompletableFuture.html)

### 设计模式
- **Proxy Pattern**: 远程代理
- **Future Pattern**: 异步结果
- **Reactor Pattern**: 事件驱动
- **Annotation Processing**: 编译时代码生成

### 类似框架对比

| 特性 | QRPC | Dubbo | gRPC |
|------|------|-------|------|
| 异步支持 | ✅ Promise | ✅ CompletableFuture | ✅ Future |
| 代码生成 | ✅ 编译时 | ❌ 运行时 | ✅ protoc |
| 传输协议 | Netty/RabbitMQ | Netty/Dubbo | HTTP/2 |
| 序列化 | Protostuff | Hessian/Kryo | Protobuf |
| 学习成本 | 低 | 高 | 中 |
| 适用场景 | 游戏/实时应用 | 企业级微服务 | 跨语言 RPC |

---

## 附录 A: 完整 API 参考

### Node API
```java
public class Node {
    Node(int id, Config config, Connector... connectors)
    Node(int id, Connector... connectors)
    
    void start()
    void stop()
    boolean isRunning()
    
    void addService(Service<?> service)
    void addService(Service<?> service, int workerId)
    void addService(Service<?> service, Worker worker)
    void addService(Service<?> service, Predicate<Worker> predicate)
    void removeService(Object serviceId)
    
    int getId()
    Config getConfig()
    Map<Integer, Worker> getWorkers()
    long getTime()
}
```

### Service API
```java
public abstract class Service<I> {
    public abstract I getId()
    
    protected void init()
    protected void destroy()
    
    Worker getWorker()
    long getTime()
    
    Timer newTimer(Runnable task, long delay)
    Timer newTimer(Runnable task, long delay, long period)
    Timer newTimer(Runnable task, String cron)
    
    <R> DelayedResult<R> newDelayedResult()
    void execute(Runnable task)
}
```

### Promise API
```java
public class Promise<R> {
    Promise<Void> thenRun(Runnable handler)
    Promise<Void> thenAccept(Consumer<? super R> handler)
    <U> Promise<U> thenApply(Function<? super R, ? extends U> handler)
    <U> Promise<U> thenCompose(Function<? super R, ? extends Promise<U>> handler)
    Promise<Void> exceptionally(Consumer<? super Throwable> handler)
    Promise<Void> completely(BiConsumer<? super R, ? super Throwable> handler)
    Promise<Void> completely(Runnable handler)
    
    boolean isDone()
    boolean isOK()
    boolean isFailed()
    R getResult()
    
    static Promise<Void> allOf(Promise<?>... promises)
    static Promise<Object> anyOf(Promise<?>... promises)
}
```

## 结语

QRPC 是一个简洁高效的 RPC 框架，特别适合对性能敏感、需要异步编程模型的实时应用场景（如游戏服务器、即时通讯等）。通过编译时代码生成避免了运行时反射开销，灵活的线程模型满足了不同业务场景的需求。

---

**[⬆ 返回顶部](#目录)**