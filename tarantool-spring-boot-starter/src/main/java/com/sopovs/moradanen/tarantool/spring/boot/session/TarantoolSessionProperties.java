package com.sopovs.moradanen.tarantool.spring.boot.session;

import org.springframework.boot.context.properties.ConfigurationProperties;

import com.sopovs.moradanen.tarantool.spring.session.TarantoolHttpSessionConfiguration;
import com.sopovs.moradanen.tarantool.spring.session.TarantoolSessionRepository;

@ConfigurationProperties(prefix = "spring.session.tarantool")
public class TarantoolSessionProperties {

	private String spaceName = TarantoolSessionRepository.DEFAULT_SPACE_NAME;

	private String cleanupCron = TarantoolHttpSessionConfiguration.DEFAULT_CLEANUP_CRON;

	private boolean createSpaces = false;

	public String getSpaceName() {
		return this.spaceName;
	}

	public void setSpaceName(String spaceName) {
		this.spaceName = spaceName;
	}

	public String getCleanupCron() {
		return this.cleanupCron;
	}

	public void setCleanupCron(String cleanupCron) {
		this.cleanupCron = cleanupCron;
	}

	public boolean isCreateSpaces() {
		return createSpaces;
	}

	public void setCreateSpaces(boolean createSpaces) {
		this.createSpaces = createSpaces;
	}
}
