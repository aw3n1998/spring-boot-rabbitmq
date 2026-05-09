package com.example.rabbitmq.consumer;

import com.example.rabbitmq.config.RabbitConfig;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Component
public class MessageConsumer {

    private static final Logger log = LoggerFactory.getLogger(MessageConsumer.class);

    @Autowired
    private RabbitTemplate rabbitTemplate;

    // 通用的处理逻辑提取
    private void processAndAck(Message message, Channel channel, String queueName) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        String receivedRoutingKey = message.getMessageProperties().getReceivedRoutingKey();
        try {
            String msgBody = new String(message.getBody());
            log.info("📥 [信箱: {}] 消费成功 | 来源RoutingKey: [{}] | 内容: {}", queueName, receivedRoutingKey, msgBody);
            channel.basicAck(deliveryTag, false);
        } catch (Exception e) {
            channel.basicNack(deliveryTag, false, true);
        }
    }

    @RabbitListener(queues = RabbitConfig.QUEUE_ALIPAY) public void listenAlipay(Message message, Channel channel) throws IOException { processAndAck(message, channel, "支付宝"); }
    @RabbitListener(queues = RabbitConfig.QUEUE_WECHAT) public void listenWechat(Message message, Channel channel) throws IOException { processAndAck(message, channel, "微信"); }
    @RabbitListener(queues = RabbitConfig.QUEUE_EMAIL) public void listenEmail(Message message, Channel channel) throws IOException { processAndAck(message, channel, "邮件"); }
    @RabbitListener(queues = RabbitConfig.QUEUE_ALL_NOTIFY) public void listenAllNotify(Message message, Channel channel) throws IOException { processAndAck(message, channel, "全局通知"); }
    @RabbitListener(queues = RabbitConfig.QUEUE_BROADCAST_A) public void listenBroadcastA(Message message, Channel channel) throws IOException { processAndAck(message, channel, "系统A广播"); }
    @RabbitListener(queues = RabbitConfig.QUEUE_BROADCAST_B) public void listenBroadcastB(Message message, Channel channel) throws IOException { processAndAck(message, channel, "系统B广播"); }


    // =====================================
    // 🌟 监听正常业务队列 (模拟产生死信)
    // =====================================
    @RabbitListener(queues = RabbitConfig.BUSINESS_QUEUE)
    public void listenBusiness(Message message, Channel channel) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        try {
            String msgBody = new String(message.getBody());
            log.info("💼 [业务信箱] 收到任务: {}", msgBody);
            
            // 模拟业务处理失败：如果消息内容包含 "error"，则抛出异常
            if (msgBody.toLowerCase().contains("error")) {
                throw new RuntimeException("发现违规数据，处理异常，准备拒收该消息！");
            }
            
            // 正常处理成功，确认消息
            channel.basicAck(deliveryTag, false);
            log.info("💼 [业务信箱] 任务完成，已确认(ACK)！");
            
        } catch (Exception e) {
            log.error("💼 [业务信箱] 异常告警：{}", e.getMessage());
            
            // 重点！！！
            // basicNack(deliveryTag, multiple, requeue)
            // requeue = false 代表：我不要这条消息了，请不要把它重新放回我的队列！
            // 因为我们在 RabbitConfig 里给这个队列配置了死信交换机，所以这条被拒绝的消息不会丢失，而是会流向死信队列！
            channel.basicNack(deliveryTag, false, false);
        }
    }

    // =====================================
    // 🌟 监听死信队列（专门处理那些“无家可归”或“无法处理”的消息）
    // =====================================
    @RabbitListener(queues = RabbitConfig.DLX_QUEUE)
    public void listenDlx(Message message, Channel channel) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        String msgBody = new String(message.getBody());
        
        log.warn("☠️ [死信信箱] 捕获到死信消息！内容: {}", msgBody);
        
        try {
            // 模拟人工干预或补偿逻辑（比如把触发错误的 "error" 关键字修复）
            if (msgBody.toLowerCase().contains("error")) {
                log.info("🔧 [死信信箱] 正在修复脏数据...");
                msgBody = msgBody.replaceAll("(?i)error", "fixed");
                
                // 【核心：动态获取原来的交换机和路由键】
                // RabbitMQ 会将死信之前的投递历史（Exchange、RoutingKey等）塞在 header 的 "x-death" 属性里。
                List<Map<String, ?>> xDeathList = message.getMessageProperties().getXDeathHeader();
                
                if (xDeathList != null && !xDeathList.isEmpty()) {
                    // 获取最近的一次死信记录
                    Map<String, ?> deathInfo = xDeathList.get(0);
                    
                    // 提取当初发消息时的 Exchange
                    String originalExchange = (String) deathInfo.get("exchange");
                    // 提取当初发消息时的 RoutingKey
                    List<String> originalRoutingKeys = (List<String>) deathInfo.get("routing-keys");
                    String originalRoutingKey = (originalRoutingKeys != null && !originalRoutingKeys.isEmpty()) ? originalRoutingKeys.get(0) : "";

                    log.info("🚀 [死信信箱] 数据修复完毕，动态解析出最初的投递地址并准备重投递 -> Exchange: [{}], RoutingKey: [{}]", originalExchange, originalRoutingKey);
                    
                    // 使用动态解析出来的参数进行重发，完美解耦，不再写死 BUSINESS_EXCHANGE
                    rabbitTemplate.convertAndSend(originalExchange, originalRoutingKey, msgBody);
                } else {
                    log.warn("⚠️ [死信信箱] 找不到 x-death 记录，无法执行动态重投递！");
                }
            } else {
                log.warn("⚠️ [死信信箱] 无法修复的数据，记录到数据库供人工排查...");
            }
            
            // 确认死信
            channel.basicAck(deliveryTag, false);
            log.info("✅ [死信信箱] 死信处理完成并确认(ACK)");
            
        } catch (Exception e) {
            log.error("❌ [死信信箱] 处理死信时发生严重异常", e);
            // 处理死信失败，选择丢弃或记录
            channel.basicNack(deliveryTag, false, false); 
        }
    }
}
