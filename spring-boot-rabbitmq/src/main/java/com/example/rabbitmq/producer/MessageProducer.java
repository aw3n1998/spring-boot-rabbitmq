package com.example.rabbitmq.producer;

import com.example.rabbitmq.config.RabbitConfig;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MessageProducer {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @GetMapping("/sendDirect")
    public String sendDirect(@RequestParam String key, @RequestParam String msg) {
        rabbitTemplate.convertAndSend(RabbitConfig.DIRECT_EXCHANGE, key, msg);
        return String.format("Direct消息已发出 | 交换机: %s | RoutingKey: %s", RabbitConfig.DIRECT_EXCHANGE, key);
    }

    @GetMapping("/sendTopic")
    public String sendTopic(@RequestParam String key, @RequestParam String msg) {
        rabbitTemplate.convertAndSend(RabbitConfig.TOPIC_EXCHANGE, key, msg);
        return String.format("Topic消息已发出 | 交换机: %s | RoutingKey: %s", RabbitConfig.TOPIC_EXCHANGE, key);
    }

    @GetMapping("/sendFanout")
    public String sendFanout(@RequestParam String msg) {
        rabbitTemplate.convertAndSend(RabbitConfig.FANOUT_EXCHANGE, "", msg);
        return String.format("Fanout广播消息已发出 | 交换机: %s | 消息内容: %s", RabbitConfig.FANOUT_EXCHANGE, msg);
    }

    // =====================================
    // 🌟 场景 D：死信队列演示
    // =====================================
    @GetMapping("/sendBusiness")
    public String sendBusiness(@RequestParam String msg) {
        // 我们正常把消息发给业务线
        rabbitTemplate.convertAndSend(RabbitConfig.BUSINESS_EXCHANGE, "business.routing.key", msg);
        
        return "业务消息已发出(如果你消息里带有'error'字样，消费者会故意报错拒收，从而触发死信机制！) | 内容: " + msg;
    }
}