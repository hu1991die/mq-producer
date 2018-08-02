package com.feizi;

import com.feizi.starter.RocketMqSendTemplate;
import com.feizi.starter.entity.MessageData;
import com.feizi.starter.entity.User;
import com.feizi.starter.util.MessageUtils;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

import javax.annotation.Resource;
import java.util.UUID;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
public class MqProducerApplication implements CommandLineRunner {
	private static final Logger LOGGER = LoggerFactory.getLogger(MqProducerApplication.class);

	/**
	 * MQ消息消费者topic定义
	 */
	private static final String MQ_TOPIC = "feizi_topic";

	@Resource
	private RocketMqSendTemplate rocketMqSendTemplate;

	public static void main(String[] args) {
		SpringApplication.run(MqProducerApplication.class, args);
	}

	@Override
	public void run(String... strings) throws Exception {
		LOGGER.info("================start生产者发送MQ消息");

		/*############################first###############################*/
		//封装MQ消息（first）
		Message message1 = MessageUtils.buildMessage(MQ_TOPIC, "tagA", "key001", new User(1, "feizi", 25));
		sendMessage(message1);

		/*############################second###############################*/
		//封装MQ消息（second）
		Message message2 = MessageUtils.buildMessage(MQ_TOPIC, "tagB", "key002", new User(2, "hello", 26));
		sendMessage(message2);

		LOGGER.info("================end生产者发送MQ消息");
	}

	/**
	 * 发送MQ消息
	 * @param message MQ消息
	 */
	private void sendMessage(Message message){
		/* 发送MQ消息, 如果失败，则尝试发送3次 */
		SendResult sendResult = rocketMqSendTemplate.syncSend(message, 3);
		if(null != sendResult){
			/**
			 * TODO 发送结果，发送完毕执行业务操作
			 */
			LOGGER.info("================sendResult: " + sendResult);
		}
	}
}
