package com.sopovs.moradanen.tarantool.spring.data;

import java.io.Serializable;

import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;

public class TarantoolRepositoryFactoryBean<T extends Repository<S, ID>, S, ID extends Serializable>
		extends RepositoryFactoryBeanSupport<T, S, ID> {

	protected TarantoolRepositoryFactoryBean(Class<? extends T> repositoryInterface) {
		super(repositoryInterface);
	}

	@Override
	protected RepositoryFactorySupport createRepositoryFactory() {
		// TODO Auto-generated method stub
		return null;
	}
}
