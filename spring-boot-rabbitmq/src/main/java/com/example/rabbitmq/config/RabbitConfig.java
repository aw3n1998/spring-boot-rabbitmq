package com.example.rabbitmq.config;

import org.springframework.amqp.core.*;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitConfig {

    // ==========================================
    // 1. 定义邮局（交换机 Exchange）
    // ==========================================
    public static final String DIRECT_EXCHANGE = "payment.direct.exchange"; 
    public static final String TOPIC_EXCHANGE = "notification.topic.exchange"; 
    public static final String FANOUT_EXCHANGE = "broadcast.fanout.exchange"; 

    @Bean("directExchange") public DirectExchange directExchange() { return new DirectExchange(DIRECT_EXCHANGE); }
    @Bean("topicExchange") public TopicExchange topicExchange() { return new TopicExchange(TOPIC_EXCHANGE); }
    @Bean("fanoutExchange") public FanoutExchange fanoutExchange() { return new FanoutExchange(FANOUT_EXCHANGE); }

    // ==========================================
    // 2. 定义信箱（队列 Queue）
    // ==========================================
    public static final String QUEUE_ALIPAY = "payment.alipay.queue";       
    public static final String QUEUE_WECHAT = "payment.wechat.queue";       
    public static final String QUEUE_EMAIL = "notify.email.queue";          
    public static final String QUEUE_ALL_NOTIFY = "notify.all.queue";       
    public static final String QUEUE_BROADCAST_A = "systemA.broadcast.queue"; 
    public static final String QUEUE_BROADCAST_B = "systemB.broadcast.queue"; 

    @Bean("alipayQueue") public Queue alipayQueue() { return QueueBuilder.durable(QUEUE_ALIPAY).quorum().build(); }
    @Bean("wechatQueue") public Queue wechatQueue() { return QueueBuilder.durable(QUEUE_WECHAT).quorum().build(); }
    @Bean("emailQueue") public Queue emailQueue() { return QueueBuilder.durable(QUEUE_EMAIL).quorum().build(); }
    @Bean("allNotifyQueue") public Queue allNotifyQueue() { return QueueBuilder.durable(QUEUE_ALL_NOTIFY).quorum().build(); }
    @Bean("broadcastQueueA") public Queue broadcastQueueA() { return QueueBuilder.durable(QUEUE_BROADCAST_A).quorum().build(); }
    @Bean("broadcastQueueB") public Queue broadcastQueueB() { return QueueBuilder.durable(QUEUE_BROADCAST_B).quorum().build(); }

    // ==========================================
    // 3. 建立绑定矩阵（Binding）
    // ==========================================
    @Bean public Binding bindAlipay(@Qualifier("alipayQueue") Queue queue, @Qualifier("directExchange") DirectExchange exchange) { return BindingBuilder.bind(queue).to(exchange).with("pay.alipay"); }
    @Bean public Binding bindWechat(@Qualifier("wechatQueue") Queue queue, @Qualifier("directExchange") DirectExchange exchange) { return BindingBuilder.bind(queue).to(exchange).with("pay.wechat"); }
    @Bean public Binding bindEmail(@Qualifier("emailQueue") Queue queue, @Qualifier("topicExchange") TopicExchange exchange) { return BindingBuilder.bind(queue).to(exchange).with("notify.email.#"); }
    @Bean public Binding bindAllNotify(@Qualifier("allNotifyQueue") Queue queue, @Qualifier("topicExchange") TopicExchange exchange) { return BindingBuilder.bind(queue).to(exchange).with("notify.#"); }
    @Bean public Binding bindEmailToDirect(@Qualifier("emailQueue") Queue queue, @Qualifier("directExchange") DirectExchange exchange) { return BindingBuilder.bind(queue).to(exchange).with("urgent.email"); }
    @Bean public Binding bindBroadcastA(@Qualifier("broadcastQueueA") Queue queue, @Qualifier("fanoutExchange") FanoutExchange exchange) { return BindingBuilder.bind(queue).to(exchange); }
    @Bean public Binding bindBroadcastB(@Qualifier("broadcastQueueB") Queue queue, @Qualifier("fanoutExchange") FanoutExchange exchange) { return BindingBuilder.bind(queue).to(exchange); }


    // ======================================================================
    // 🌟 新增：【死信队列 (Dead Letter Queue) 机制】
    // 核心思想：给正常的队列绑定一个“收尸人”(死信交换机)，遇到无法处理的消息就丢给它。
    // ======================================================================

    public static final String DLX_EXCHANGE = "dlx.exchange"; // 死信交换机（收尸人）
    public static final String DLX_QUEUE = "dlx.queue";       // 死信队列（停尸房）
    
    public static final String BUSINESS_EXCHANGE = "business.exchange"; // 正常业务交换机
    public static final String BUSINESS_QUEUE = "business.queue";       // 正常业务队列

    // 1. 声明死信交换机和死信队列，并将它们绑定
    @Bean("dlxExchange") public DirectExchange dlxExchange() { return new DirectExchange(DLX_EXCHANGE); }
    @Bean("dlxQueue") public Queue dlxQueue() { return QueueBuilder.durable(DLX_QUEUE).quorum().build(); }
    @Bean
    public Binding bindDlx(@Qualifier("dlxQueue") Queue queue, @Qualifier("dlxExchange") DirectExchange exchange) {
        return BindingBuilder.bind(queue).to(exchange).with("dlx.routing.key");
    }

    // 2. 声明正常业务队列，【关键：为其配置死信参数！】
    @Bean("businessQueue")
    public Queue businessQueue() {
        return QueueBuilder.durable(BUSINESS_QUEUE)
                .quorum() // 设置为仲裁队列！
                // 指定：如果这队列里的消息变成死信了，把它丢到哪个交换机（收尸人）
                .deadLetterExchange(DLX_EXCHANGE) 
                // 指定：丢过去的时候，附带什么新的 RoutingKey，以便被正确路由到死信队列
                .deadLetterRoutingKey("dlx.routing.key") 
                // (可选)：还可以设置队列级消息过期时间(TTL)，单位毫秒。超时未消费自动变成死信。
                // .ttl(10000) 
                .build();
    }

    // 3. 声明正常业务交换机，并绑定正常业务队列
    @Bean("businessExchange") public DirectExchange businessExchange() { return new DirectExchange(BUSINESS_EXCHANGE); }
    @Bean
    public Binding bindBusiness(@Qualifier("businessQueue") Queue queue, @Qualifier("businessExchange") DirectExchange exchange) {
        return BindingBuilder.bind(queue).to(exchange).with("business.routing.key");
    }
}