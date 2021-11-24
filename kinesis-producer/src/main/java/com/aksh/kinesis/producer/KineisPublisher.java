package com.aksh.kinesis.producer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.google.gson.Gson;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.kinesis.common.KinesisClientUtil;

@Component
class KineisPublisher {

	@Autowired
	private Region region;

	@Value("${streamName:aksh-first}")
	String streamName = "aksh-first";

	@Value("${publishType:api}")
	String type = "api";

	String partitionPrefix = "partitionPrefix";

	
	Gson gson=new Gson();
	
	@Value("${intervalMs:100}")
	int intervalMs=100;
	
	@Autowired
	RandomGenerator randomGenerator;
	
	@Value("${aggregationEnabled:false}")
	private boolean aggregationEnabled;

	

	@PostConstruct
	void pubish() throws Exception {

		if (Optional.ofNullable(type).orElse("api").equalsIgnoreCase("api")) {
			publishAPI();
		} else {
			publishKPL();
		}

	}

	private void publishKPL() throws IOException {
		// KinesisProducer gets credentials automatically like
		// DefaultAWSCredentialsProviderChain.
		// It also gets region automatically from the EC2 metadata service.
		KinesisProducerConfiguration config = new KinesisProducerConfiguration().setAggregationEnabled(aggregationEnabled)
				.setRecordMaxBufferedTime(3000).setMaxConnections(1).setRequestTimeout(60000);
		config.setRegion(region.id());
		KinesisProducer kinesis = new KinesisProducer(config);
		
		// Put some records
		int i = 0;
		while (true) {
			String payload = randomGenerator.createPayload();
			ByteBuffer data = ByteBuffer.wrap(payload.getBytes("UTF-8"));
			// doesn't block
			kinesis.addUserRecord(streamName, partitionPrefix + i % 4, data);
			sleep();
			i++;
		}
	}

	private void publishAPI() {
		KinesisAsyncClient client = KinesisClientUtil
				.createKinesisAsyncClient(KinesisAsyncClient.builder().region(region));

		Executors.newCachedThreadPool().execute(() -> {
			int i = 0;

			while (true) {
				String payload;
				try {
					payload = randomGenerator.createPayload();
					PutRecordRequest req = PutRecordRequest.builder().streamName(streamName)
							.data(SdkBytes.fromUtf8String(payload)).partitionKey(partitionPrefix +i%2 ).build();
					client.putRecord(req);
					sleep();
					i++;
				} catch (IOException e) {
					e.printStackTrace();
				}
				
			}
		});
	}

	private void sleep() {
		try {
			Thread.sleep(intervalMs);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
