package com.switchvov.magicmq.server;

import com.switchvov.magicmq.model.Message;
import com.switchvov.magicmq.model.Result;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * MQ server.
 *
 * @author switch
 * @since 2024/07/03
 */
@RestController
@RequestMapping("/magicmq")
public class MQServer {

    @PostMapping("/send")
    public Result<String> send(@RequestParam("t") String topic,
                               @RequestBody Message<String> message) {
        return Result.ok("" + MessageQueue.send(topic, message));
    }

    @RequestMapping("/recv")
    public Result<Message<?>> recv(@RequestParam("t") String topic,
                                   @RequestParam("cid") String consumerId) {
        return Result.msg(MessageQueue.recv(topic, consumerId));
    }

    @RequestMapping("/batch")
    public Result<List<Message<?>>> batch(@RequestParam("t") String topic,
                                          @RequestParam("cid") String consumerId,
                                          @RequestParam(name = "size", required = false, defaultValue = "1000") int size) {
        return Result.msg(MessageQueue.batch(topic, consumerId, size));
    }

    @RequestMapping("/ack")
    public Result<String> ack(@RequestParam("t") String topic,
                              @RequestParam("cid") String consumerId,
                              @RequestParam("offset") Integer offset) {
        return Result.ok("" + MessageQueue.ack(topic, consumerId, offset));
    }

    @RequestMapping("/sub")
    public Result<String> sub(@RequestParam("t") String topic,
                              @RequestParam("cid") String consumerId) {
        MessageQueue.sub(new MessageSubscription(topic, consumerId, -1));
        return Result.ok();
    }

    @RequestMapping("/unsub")
    public Result<String> unsub(@RequestParam("t") String topic,
                                @RequestParam("cid") String consumerId) {
        MessageQueue.unsub(new MessageSubscription(topic, consumerId, -1));
        return Result.ok();
    }
}
