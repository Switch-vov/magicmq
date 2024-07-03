package com.switchvov.magicmq.server;

import com.switchvov.magicmq.model.MagicMessage;
import com.switchvov.magicmq.model.Result;
import org.springframework.web.bind.annotation.*;

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
                               @RequestParam("cid") String consumerId,
                               @RequestBody MagicMessage<String> message) {
        return Result.ok("" + MessageQueue.send(topic, consumerId, message));
    }

    @RequestMapping("/recv")
    public Result<MagicMessage<?>> recv(@RequestParam("t") String topic,
                                        @RequestParam("cid") String consumerId) {
        return Result.msg(MessageQueue.recv(topic, consumerId));
    }

    @RequestMapping("/ack")
    public Result<String> ack(@RequestParam("t") String topic,
                              @RequestParam("cid") String consumerId,
                              @RequestParam("offset") Integer offest) {
        return Result.ok("" + MessageQueue.ack(topic, consumerId, offest));
    }

    @RequestMapping("/sub")
    public Result<String> subscribe(@RequestParam("t") String topic,
                                    @RequestParam("cid") String consumerId) {
        MessageQueue.sub(new MessageSubscription(topic, consumerId, 0));
        return Result.ok();
    }

    @RequestMapping("/unsub")
    public Result<String> unsubscribe(@RequestParam("t") String topic,
                                      @RequestParam("cid") String consumerId) {
        MessageQueue.unsub(new MessageSubscription(topic, consumerId, -1));
        return Result.ok();
    }
}
