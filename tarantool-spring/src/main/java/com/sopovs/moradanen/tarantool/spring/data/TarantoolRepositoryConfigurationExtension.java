package com.sopovs.moradanen.tarantool.spring.data;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.Collections;

import org.springframework.data.repository.config.RepositoryConfigurationExtensionSupport;

import com.sopovs.moradanen.tarantool.spring.data.mapping.Schema;

public class TarantoolRepositoryConfigurationExtension extends RepositoryConfigurationExtensionSupport {


	@Override
	public String getModuleName() {
		return "Tarantool";
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.springframework.data.repository.config.
	 * RepositoryConfigurationExtensionSupport#getModulePrefix()
	 */
	@Override
	protected String getModulePrefix() {
		return "tarantool";
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.springframework.data.repository.config.
	 * RepositoryConfigurationExtension#getRepositoryFactoryClassName()
	 */
	public String getRepositoryFactoryClassName() {
		return TarantoolRepositoryFactoryBean.class.getName();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.springframework.data.repository.config.
	 * RepositoryConfigurationExtensionSupport#getIdentifyingAnnotations()
	 */
	@Override
	protected Collection<Class<? extends Annotation>> getIdentifyingAnnotations() {
		return Collections.<Class<? extends Annotation>> singleton(Schema.class);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.springframework.data.repository.config.
	 * RepositoryConfigurationExtensionSupport#getIdentifyingTypes()
	 */
	@Override
	protected Collection<Class<?>> getIdentifyingTypes() {
		return Collections.<Class<?>> singleton(TarantoolRepository.class);
	}


}
