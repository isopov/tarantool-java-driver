package com.sopovs.moradanen.tarantool.spring.data;

import java.io.Serializable;

import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.NoRepositoryBean;

@NoRepositoryBean
public interface TarantoolRepository<T, ID extends Serializable> extends CrudRepository<T, ID> {

}
