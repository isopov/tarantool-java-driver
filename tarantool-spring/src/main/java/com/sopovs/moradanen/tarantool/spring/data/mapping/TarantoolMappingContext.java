package com.sopovs.moradanen.tarantool.spring.data.mapping;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;

import org.springframework.data.mapping.context.AbstractMappingContext;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.util.TypeInformation;

import com.sopovs.moradanen.tarantool.TarantoolTemplate;

public class TarantoolMappingContext
		extends AbstractMappingContext<BasicTarantoolPersistentEntity<?>, TarantoolPersistentProperty> {

	private final TarantoolTemplate tarantoolTemplate;

	public TarantoolMappingContext(TarantoolTemplate tarantoolTemplate) {
		this.tarantoolTemplate = tarantoolTemplate;
	}

	@Override
	protected <T> BasicTarantoolPersistentEntity<T> createPersistentEntity(TypeInformation<T> typeInformation) {
		return new BasicTarantoolPersistentEntity<>(typeInformation, tarantoolTemplate);
	}

	@Override
	protected TarantoolPersistentProperty createPersistentProperty(Field field, PropertyDescriptor descriptor,
			BasicTarantoolPersistentEntity<?> owner, SimpleTypeHolder simpleTypeHolder) {
		return new BasicTarantoolPersistentProperty(field, descriptor, owner, simpleTypeHolder);
	}

}
