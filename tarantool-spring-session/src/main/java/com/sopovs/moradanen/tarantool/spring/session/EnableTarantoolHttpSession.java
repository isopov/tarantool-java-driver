package com.sopovs.moradanen.tarantool.spring.session;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.session.MapSession;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Documented
@Import(TarantoolHttpSessionConfiguration.class)
@Configuration
public @interface EnableTarantoolHttpSession {

	/**
	 * The session timeout in seconds. By default, it is set to 1800 seconds (30
	 * minutes). This should be a non-negative integer.
	 * 
	 * @return the seconds a session can be inactive before expiring
	 */
	int maxInactiveIntervalInSeconds() default MapSession.DEFAULT_MAX_INACTIVE_INTERVAL_SECONDS;

	/**
	 * The name of Tarantool space used by Spring Session to store sessions.
	 * 
	 * @return the Tarantool space name
	 */
	String spaceName() default TarantoolSessionRepository.DEFAULT_SPACE_NAME;

	/**
	 * The cron expression for expired session cleanup job. By default runs every
	 * minute.
	 * 
	 * @return the session cleanup cron expression
	 */
	String cleanupCron() default TarantoolHttpSessionConfiguration.DEFAULT_CLEANUP_CRON;

}
