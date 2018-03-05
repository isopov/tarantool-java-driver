package com.sopovs.moradanen.tarantool.spring.session;

import java.util.Map;

import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.EmbeddedValueResolverAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportAware;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.core.serializer.support.DeserializingConverter;
import org.springframework.core.serializer.support.SerializingConverter;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.session.MapSession;
import org.springframework.session.config.annotation.web.http.SpringHttpSessionConfiguration;
import org.springframework.session.web.http.SessionRepositoryFilter;
import org.springframework.util.StringUtils;
import org.springframework.util.StringValueResolver;

import com.sopovs.moradanen.tarantool.TarantoolClientSource;

/**
 * Spring {@code @Configuration} class used to configure and initialize a
 * Tarantool based {@code HttpSession} provider implementation in Spring
 * Session.
 * <p>
 * Exposes the {@link SessionRepositoryFilter} as a bean named
 * {@code springSessionRepositoryFilter}. In order to use this a single
 * {@link TarantoolClientSource} must be exposed as a Bean.
 *
 * @see EnableTarantoolHttpSession
 */
@Configuration
@EnableScheduling
public class TarantoolHttpSessionConfiguration extends SpringHttpSessionConfiguration
		implements BeanClassLoaderAware, EmbeddedValueResolverAware, ImportAware, SchedulingConfigurer {

	public static final String DEFAULT_CLEANUP_CRON = "0 * * * * *";

	private Integer maxInactiveIntervalInSeconds = MapSession.DEFAULT_MAX_INACTIVE_INTERVAL_SECONDS;

	private String spaceName = TarantoolSessionRepository.DEFAULT_SPACE_NAME;

	private String cleanupCron = DEFAULT_CLEANUP_CRON;

	private boolean createSpaces = false;

	private ConversionService springSessionConversionService;

	private ConversionService conversionService;

	private ClassLoader classLoader;

	private StringValueResolver embeddedValueResolver;

	private TarantoolClientSource clientSource;

	@Bean
	public TarantoolSessionRepository sessionRepository() {
		TarantoolSessionRepository sessionRepository = new TarantoolSessionRepository(clientSource);
		if (StringUtils.hasText(this.spaceName)) {
			sessionRepository.setSpaceName(this.spaceName);
		}
		sessionRepository.setDefaultMaxInactiveInterval(this.maxInactiveIntervalInSeconds);

		if (this.springSessionConversionService != null) {
			sessionRepository.setConversionService(this.springSessionConversionService);
		} else if (this.conversionService != null) {
			sessionRepository.setConversionService(this.conversionService);
		} else {
			sessionRepository.setConversionService(createConversionServiceWithBeanClassLoader());
		}

		if (createSpaces) {
			sessionRepository.createSpaces();
		}
		return sessionRepository;
	}

	public void setMaxInactiveIntervalInSeconds(Integer maxInactiveIntervalInSeconds) {
		this.maxInactiveIntervalInSeconds = maxInactiveIntervalInSeconds;
	}

	public void setSpaceName(String spaceName) {
		this.spaceName = spaceName;
	}

	public void setCleanupCron(String cleanupCron) {
		this.cleanupCron = cleanupCron;
	}

	public void setCreateSpaces(boolean createSpaces) {
		this.createSpaces = createSpaces;
	}

	@Autowired
	public void setTarantoolClientSource(TarantoolClientSource clientSource) {
		this.clientSource = clientSource;
	}

	@Autowired(required = false)
	@Qualifier("springSessionConversionService")
	public void setSpringSessionConversionService(ConversionService conversionService) {
		this.springSessionConversionService = conversionService;
	}

	@Autowired(required = false)
	@Qualifier("conversionService")
	public void setConversionService(ConversionService conversionService) {
		this.conversionService = conversionService;
	}

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader;
	}

	@Override
	public void setEmbeddedValueResolver(StringValueResolver resolver) {
		this.embeddedValueResolver = resolver;
	}

	@Override
	public void setImportMetadata(AnnotationMetadata importMetadata) {
		Map<String, Object> attributeMap = importMetadata
				.getAnnotationAttributes(EnableTarantoolHttpSession.class.getName());
		AnnotationAttributes attributes = AnnotationAttributes.fromMap(attributeMap);
		this.maxInactiveIntervalInSeconds = attributes.getNumber("maxInactiveIntervalInSeconds");
		String spaceNameValue = attributes.getString("spaceName");
		if (StringUtils.hasText(spaceNameValue)) {
			this.spaceName = this.embeddedValueResolver.resolveStringValue(spaceNameValue);
		}
		String cleanupCron = attributes.getString("cleanupCron");
		if (StringUtils.hasText(cleanupCron)) {
			this.cleanupCron = cleanupCron;
		}
	}

	@Override
	public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
		taskRegistrar.addCronTask(() -> sessionRepository().cleanUpExpiredSessions(), this.cleanupCron);
	}

	private GenericConversionService createConversionServiceWithBeanClassLoader() {
		GenericConversionService conversionService = new GenericConversionService();
		conversionService.addConverter(Object.class, byte[].class, new SerializingConverter());
		conversionService.addConverter(byte[].class, Object.class, new DeserializingConverter(this.classLoader));
		return conversionService;
	}

}
