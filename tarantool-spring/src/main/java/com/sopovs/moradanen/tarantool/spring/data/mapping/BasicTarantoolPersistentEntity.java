package com.sopovs.moradanen.tarantool.spring.data.mapping;

import java.util.Objects;

import org.springframework.data.mapping.model.BasicPersistentEntity;
import org.springframework.data.util.TypeInformation;

import com.sopovs.moradanen.tarantool.TarantoolTemplate;

public class BasicTarantoolPersistentEntity<T> extends BasicPersistentEntity<T, TarantoolPersistentProperty> implements
		TarantoolPersistentEntity<T> {

	private final int id;

	public BasicTarantoolPersistentEntity(TypeInformation<T> information, TarantoolTemplate tarantoolTemplate) {
		super(information);

		// TODO - something other than NPE?
		Schema schema = Objects.requireNonNull(findAnnotation(Schema.class));
		if (schema.id() == 0) {
			id = tarantoolTemplate.space(schema.value());
		} else {
			id = schema.id();
		}
	}

	@Override
	public int getId() {
		return id;
	}
}
