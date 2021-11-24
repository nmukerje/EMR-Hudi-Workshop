package com.aksh.kinesis.producer;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.json.GsonJsonParser;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.StringUtils;

import software.amazon.awssdk.utils.IoUtils;

@Component
public class RandomGenerator {
	
	AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
	
	String template;
	
	@Value("${bucketName:aksh-test-versioning}")
	String bucket = "aksh-test-versioning";
	
	@Value("${templatePath:kinesis/payload/file.txt}")
	String templatePath="kinesis/payload/file.txt";
	
	@Value("${randomizedProperties:null}")
	String randomizedProperties;
	
	Map<String,List<String>> mapOfRandomizedValues;
	
	@Autowired
	private Environment env;
	
	static Random random = new Random();
	
	private DateFormat dateFormate=new SimpleDateFormat("yyMMddHHmm");

	@PostConstruct
	void pubish() throws Exception {
		template = readTemplate();
		System.out.println(env.getProperty("symbols"));
		List<String> randomizedSymols=Optional.ofNullable(randomizedProperties).map(s->s.split(",")).map(Arrays::asList).orElse(Collections.EMPTY_LIST);
		mapOfRandomizedValues=randomizedSymols.stream().collect(Collectors.toMap(s->s, this::getListOfProperty));
		System.out.println(mapOfRandomizedValues);

	}
	
	private List<String> getListOfProperty(String propertyName){
		return Optional.ofNullable(env.getProperty(propertyName)).map(s->s.split(",")).map(Arrays::asList).orElse(Collections.EMPTY_LIST);
	}
	
	public String createPayload() throws IOException {
		String payload= randomize(template);
		System.out.println("Pushing Record " + payload+"testing");
		return payload;
	}

	private String readTemplate() throws IOException {
		String template = "testData-" + System.currentTimeMillis();
		try {
			if(StringUtils.isNullOrEmpty(bucket)) {
				template=FileUtils.readFileToString(new File(templatePath));
			}else {
				S3Object object = s3.getObject(bucket, templatePath);
				template = IoUtils.toUtf8String(object.getObjectContent().getDelegateStream());
				String time = Optional.ofNullable(new GsonJsonParser().parseMap(template).get("time"))
						.orElse(template.substring("testData-".length())).toString();
				System.out.println("Time from template:"+template+", is:"+time);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		} 
		
		return template;
	}

	private String randomize(String template) {
		for (int i = 10; i > 1; i--) {
			template=template.replaceAll("RANDOM_TEXT"+i, generateRandome("RANDOM_TEXT"+i));
			template=template.replaceAll("RANDOM_INT"+i, generateRandome("RANDOM_INT"+i));
			template=template.replaceAll("RANDOM_FLOAT"+i, generateRandome("RANDOM_FLOAT"+i));
		}
		for (String keyS : mapOfRandomizedValues.keySet()) {
			template=template.replaceAll("RANDOM_"+keyS, generateRandomeSymbols(mapOfRandomizedValues.get(keyS)));
		}
		
		template=template.replaceAll("RANDOM_EPOCH",randomEpoch()+"");
		template=template.replaceAll("EPOCH", System.currentTimeMillis()/1000+"");
		template=template.replaceAll("DATE_yyMMddHHmm", getEPOCH_yyMMddHHmm());
		template=template.replaceAll("DATE_STRING", new Date()+"");
		
		
		return template;

	}
	
	private String getEPOCH_yyMMddHHmm() {
		return dateFormate.format(new Date());
	}
	
	
	private long randomEpoch() {
		if(random.nextInt(100)<5) {
			return System.currentTimeMillis()/1000-random.nextInt(7200);	
		}else {
			return System.currentTimeMillis()/1000;
		}
		
	}
	
	private String generateRandomeSymbols(final List<String> values) {
		return values.get(random.nextInt(values.size()));
	}

	public static String generateRandome(String templateText) {
		if(templateText.startsWith("RANDOM_TEXT")) {
			int length=Integer.valueOf(templateText.replaceAll("RANDOM_TEXT", "").trim());
			return generateRandomString(length);
			
		}else if(templateText.startsWith("RANDOM_INT")) {
			int length=Integer.valueOf(templateText.replaceAll("RANDOM_INT", "").trim());
			return generateRandomInt(length);
			
		}else if(templateText.startsWith("RANDOM_FLOAT")) {
			int length=Integer.valueOf(templateText.replaceAll("RANDOM_FLOAT", "").trim());
			return generateRandomDouble(length);
			
		}else {
			return templateText;
		}
		
	}


	public static String generateRandomString(int targetStringLength) {
		int leftLimit = 97; // letter 'a'
		int rightLimit = 122; // letter 'z'
		return generateRandom(targetStringLength,leftLimit,rightLimit);
	}

	private static String generateRandom(int targetStringLength,int leftLimit,int rightLimit) {
		

		String generatedString = random.ints(leftLimit, rightLimit + 1).limit(targetStringLength)
				.collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();

		return generatedString;
	}
	
	public static String generateRandomInt(int targetStringLength) {
		int leftLimit = '0'; // letter 'a'
		int rightLimit = '9'; // letter 'z'
		return generateRandom(targetStringLength,leftLimit,rightLimit);
	}
	
	public static String generateRandomDouble(int targetStringLength) {
		
		int leftLimit = '0'; // letter 'a'
		int rightLimit = '9'; // letter 'z'
		return generateRandom(targetStringLength,leftLimit,rightLimit)+".0";

	}

}
