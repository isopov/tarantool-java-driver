package com.sopovs.moradanen.tarantool.spring.boot.session;

import java.time.Duration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.session.SessionProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.session.SessionRepository;

import com.sopovs.moradanen.tarantool.TarantoolClientSource;
import com.sopovs.moradanen.tarantool.spring.session.TarantoolHttpSessionConfiguration;
import com.sopovs.moradanen.tarantool.spring.session.TarantoolSessionRepository;

@Configuration
@ConditionalOnClass({ TarantoolClientSource.class, TarantoolSessionRepository.class })
@ConditionalOnMissingBean(SessionRepository.class)
@EnableConfigurationProperties(TarantoolSessionProperties.class)
public class TarantoolSessionAutoConfiguration {

	@Configuration
	public static class SpringBootTarantoolHttpSessionConfiguration
			extends TarantoolHttpSessionConfiguration {

		@Autowired
		public void customize(SessionProperties sessionProperties,
				TarantoolSessionProperties tarantoolSessionProperties) {
			Duration timeout = sessionProperties.getTimeout();
			if (timeout != null) {
				setMaxInactiveIntervalInSeconds((int) timeout.getSeconds());
			}
			setSpaceName(tarantoolSessionProperties.getSpaceName());
			setCleanupCron(tarantoolSessionProperties.getCleanupCron());
			tarantoolSessionProperties.isCreateSpaces();
		}

	}

}
