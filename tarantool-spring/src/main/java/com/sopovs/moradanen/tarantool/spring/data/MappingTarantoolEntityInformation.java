package com.sopovs.moradanen.tarantool.spring.data;

import java.io.Serializable;

import org.springframework.data.repository.core.support.PersistentEntityInformation;

import com.sopovs.moradanen.tarantool.spring.data.mapping.TarantoolPersistentEntity;

public class MappingTarantoolEntityInformation<T, ID extends Serializable> extends PersistentEntityInformation<T, ID>
		implements TarantoolEntityInformation<T, ID> {
	private final TarantoolPersistentEntity<T> entity;

	private MappingTarantoolEntityInformation(TarantoolPersistentEntity<T> entity) {
		super(entity);
		this.entity = entity;
	}

	@Override
	public int getSpaceId() {
		return entity.getId();
	}
}
