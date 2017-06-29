package com.sopovs.moradanen.tarantool.spring.data;

import java.lang.annotation.Annotation;

import org.springframework.data.repository.config.RepositoryBeanDefinitionRegistrarSupport;
import org.springframework.data.repository.config.RepositoryConfigurationExtension;

class TarantoolRepositoriesRegistrar extends RepositoryBeanDefinitionRegistrarSupport {

	@Override
	protected Class<? extends Annotation> getAnnotation() {
		return EnableTarantoolRepositories.class;
	}

	@Override
	protected RepositoryConfigurationExtension getExtension() {
		return new TarantoolRepositoryConfigurationExtension();
	}
}