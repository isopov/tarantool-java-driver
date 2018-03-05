package com.sopovs.moradanen.tarantool.spring.boot;

import javax.annotation.PreDestroy;

import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.sopovs.moradanen.tarantool.TarantoolTemplate;
import com.sopovs.moradanen.tarantool.TarantoolClientSource;
import com.sopovs.moradanen.tarantool.TarantoolConfig;
import com.sopovs.moradanen.tarantool.TarantoolPooledClientSource;

@Configuration
@EnableConfigurationProperties(TarantoolProperties.class)
@ConditionalOnClass(TarantoolTemplate.class)
public class TarantoolAutoConfiguration {

	private final TarantoolProperties properties;
	private TarantoolClientSource clientSource;

	@Bean
	@ConditionalOnMissingBean
	public TarantoolClientSource tarantoolClientSource() {
		TarantoolConfig config = new TarantoolConfig(properties.getHost(), properties.getPort(),
				properties.getUsername(), properties.getPassword());
		return clientSource = new TarantoolPooledClientSource(config, properties.getPoolSize());
	}

	@Bean
	@ConditionalOnMissingBean
	public TarantoolTemplate tarantoolTemplate(TarantoolClientSource tarantoolClientSource) {
		return new TarantoolTemplate(tarantoolClientSource);
	}

	public TarantoolAutoConfiguration(TarantoolProperties properties) {
		this.properties = properties;
	}

	@PreDestroy
	public void close() {
		if (clientSource != null) {
			clientSource.close();
		}
	}
}
