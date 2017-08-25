package com.sopovs.moradanen.tarantool.spring.data;

import java.io.Serializable;

import com.sopovs.moradanen.tarantool.TarantoolTemplate;

public class SimpleTarantoolRepository<T, ID extends Serializable> implements TarantoolRepository<T, ID> {

	private final TarantoolTemplate template;
	private final TarantoolEntityInformation<T, ID> entityInformation;

	public SimpleTarantoolRepository(TarantoolTemplate template, TarantoolEntityInformation<T, ID> entityInformation) {
		this.template = template;
		this.entityInformation = entityInformation;
	}

	@Override
	public <S extends T> S save(S entity) {
		
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <S extends T> Iterable<S> save(Iterable<S> entities) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public T findOne(ID id) {
//		template.selectAll(space, extractor)
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean exists(ID id) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Iterable<T> findAll() {
//		template.selectAll(entityInformation.getSpaceId(), extractor)
		return null;
	}

	@Override
	public Iterable<T> findAll(Iterable<ID> ids) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long count() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void delete(ID id) {
		// TODO Auto-generated method stub

	}

	@Override
	public void delete(T entity) {
		// TODO Auto-generated method stub

	}

	@Override
	public void delete(Iterable<? extends T> entities) {
		// TODO Auto-generated method stub

	}

	@Override
	public void deleteAll() {
		// TODO Auto-generated method stub
	}
}
