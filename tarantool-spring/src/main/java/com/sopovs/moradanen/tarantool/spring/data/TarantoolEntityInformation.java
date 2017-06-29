package com.sopovs.moradanen.tarantool.spring.data;

import java.io.Serializable;

import org.springframework.data.repository.core.EntityInformation;

public interface TarantoolEntityInformation<T, ID extends Serializable> extends EntityInformation<T, ID> {
	int getSchemaId();
}