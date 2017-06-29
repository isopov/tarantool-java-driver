package com.sopovs.moradanen.tarantool.spring.data.mapping;

import org.springframework.data.mapping.PersistentEntity;

public interface TarantoolPersistentEntity<T> extends PersistentEntity<T, TarantoolPersistentProperty> {

	int getId();
}
