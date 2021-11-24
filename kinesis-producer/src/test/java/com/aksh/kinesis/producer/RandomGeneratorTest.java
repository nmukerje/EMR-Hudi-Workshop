package com.aksh.kinesis.producer;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class RandomGeneratorTest {

	@Test
	void test() {
		System.out.println(RandomGenerator.generateRandome("RANDOM_FLOAT5"));
	}

}
