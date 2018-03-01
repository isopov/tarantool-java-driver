package com.sopovs.moradanen.tarantool;

import java.util.ArrayList;
import java.util.List;

public class TarantoolTemplate {

	private final TarantoolClientSource clientSource;

	public TarantoolTemplate(TarantoolClientSource clientSource) {
		this.clientSource = clientSource;
	}

	public void ping() {
		try (TarantoolClient client = clientSource.getClient()) {
			client.ping();
		}
	}

	public int space(String space) {
		try (TarantoolClient client = clientSource.getClient()) {
			return client.space(space);
		}
	}

	public <T> T selectAll(int space, ResultExtractor<T> extractor) {
		try (TarantoolClient client = clientSource.getClient()) {
			client.selectAll(space);
			return extractor.extract(client.execute());
		}
	}

	public <T> List<T> selectAndMapAll(int space, ResultRowMapper<T> mapper) {
		return selectAll(space, new RowMapperResultExtractor<>(mapper));
	}

	public interface ResultExtractor<T> {
		T extract(Result result);
	}

	public interface ResultRowMapper<T> {
		T map(Result result);
	}

	public static class RowMapperResultExtractor<T> implements ResultExtractor<List<T>> {
		private final ResultRowMapper<T> mapper;

		public RowMapperResultExtractor(ResultRowMapper<T> mapper) {
			this.mapper = mapper;
		}

		@Override
		public List<T> extract(Result result) {
			List<T> list = new ArrayList<>(result.getSize());
			while (result.hasNext()) {
				result.next();
				list.add(mapper.map(result));
			}
			return list;
		}
	}

}
