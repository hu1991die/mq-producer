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
	private static final String MQ_TOPIC1 = "feizi_topic1";
	private static final String MQ_TOPIC2 = "feizi_topic2";

	@Resource
	private RocketMqSendTemplate rocketMqSendTemplate;

	public static void main(String[] args) {
		SpringApplication.run(MqProducerApplication.class, args);
	}

	@Override
	public void run(String... strings) throws Exception {
		LOGGER.info("================start生产者发送MQ消息");

		//first
		MessageData<User> messageData1 = new MessageData<>();
		messageData1.setUuid(UUID.randomUUID().toString());
		messageData1.setTimestamp(System.currentTimeMillis());
		User user1 = new User(1, "feizi", 25);
		messageData1.setData(user1);

		//second
		MessageData<User> messageData2 = new MessageData<>();
		messageData2.setUuid(UUID.randomUUID().toString());
		messageData2.setTimestamp(System.currentTimeMillis());
		User user2 = new User(2, "hello", 26);
		messageData2.setData(user2);

		//封装MQ消息（first）
		Message message1 = MessageUtils.wrapMessage(MQ_TOPIC1, messageData1);

		//发送MQ消息
		SendResult sendResult1 = rocketMqSendTemplate.syncSend(message1);
		if(null != sendResult1){
			/**
			 * TODO 发送结果，发送完毕执行业务操作
			 */
			LOGGER.info("================sendResult1: " + sendResult1);
		}

		/*##################################################################*/

		//封装MQ消息（second）
		Message message2 = MessageUtils.wrapMessage(MQ_TOPIC2, messageData2);

		//发送MQ消息, 如果失败，则尝试发送3次
		SendResult sendResult2 = rocketMqSendTemplate.syncSend(message2, 3);
		if(null != sendResult2){
			/**
			 * TODO 发送结果，发送完毕执行业务操作
			 */
			LOGGER.info("================sendResult2: " + sendResult2);
		}
		LOGGER.info("================end生产者发送MQ消息");
	}
}
