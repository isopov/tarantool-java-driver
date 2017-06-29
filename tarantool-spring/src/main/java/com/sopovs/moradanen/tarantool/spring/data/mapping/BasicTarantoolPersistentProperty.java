package com.sopovs.moradanen.tarantool.spring.data.mapping;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;

import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.model.AnnotationBasedPersistentProperty;
import org.springframework.data.mapping.model.SimpleTypeHolder;

public class BasicTarantoolPersistentProperty extends AnnotationBasedPersistentProperty<TarantoolPersistentProperty>
		implements TarantoolPersistentProperty {

	public BasicTarantoolPersistentProperty(Field field, PropertyDescriptor propertyDescriptor,
			PersistentEntity<?, TarantoolPersistentProperty> owner, SimpleTypeHolder simpleTypeHolder) {
		super(field, propertyDescriptor, owner, simpleTypeHolder);
	}

	@Override
	protected Association<TarantoolPersistentProperty> createAssociation() {

		return new Association<TarantoolPersistentProperty>(this, null);
	}

}
