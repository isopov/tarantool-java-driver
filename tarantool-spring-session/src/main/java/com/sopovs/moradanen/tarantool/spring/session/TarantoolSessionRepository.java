package com.sopovs.moradanen.tarantool.spring.session;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.core.serializer.support.DeserializingConverter;
import org.springframework.core.serializer.support.SerializingConverter;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.session.FindByIndexNameSessionRepository;
import org.springframework.session.MapSession;
import org.springframework.session.Session;
import org.springframework.util.Assert;

import com.sopovs.moradanen.tarantool.Result;
import com.sopovs.moradanen.tarantool.TarantoolClient;
import com.sopovs.moradanen.tarantool.TarantoolClientSource;
import com.sopovs.moradanen.tarantool.core.IntOp;
import com.sopovs.moradanen.tarantool.core.Op;

public class TarantoolSessionRepository implements
		FindByIndexNameSessionRepository<TarantoolSessionRepository.TarantoolSession> {

	public static final String DEFAULT_SPACE_NAME = "SPRING_SESSION";
	public static final String DEFAULT_ATTRIBUTES_SPACE_NAME = DEFAULT_SPACE_NAME + "_ATTRS";

	private static final String SPRING_SECURITY_CONTEXT = "SPRING_SECURITY_CONTEXT";

	private static final Log logger = LogFactory.getLog(TarantoolSessionRepository.class);

	private static final PrincipalNameResolver PRINCIPAL_NAME_RESOLVER = new PrincipalNameResolver();

	private final TarantoolClientSource clientSource;
	private String spaceName = DEFAULT_SPACE_NAME;
	private Integer space = null;
	private String attributesSpaceName = DEFAULT_ATTRIBUTES_SPACE_NAME;
	private Integer attributesSpace = null;

	/**
	 * If non-null, this value is used to override the default value for
	 * {@link TarantoolSession#setMaxInactiveInterval(Duration)}.
	 */
	private Integer defaultMaxInactiveInterval;

	private ConversionService conversionService;

	public TarantoolSessionRepository(TarantoolClientSource clientSource) {
		Assert.notNull(clientSource, "TarantoolClientSource must not be null");
		this.clientSource = clientSource;
	}

	public void setSpaceName(String spaceName) {
		Assert.hasText(spaceName, "Space name must not be empty");
		this.spaceName = spaceName;
		this.space = null;
	}

	public void setSpace(int space) {
		this.space = space;
	}

	public void setAttributesSpaceName(String attributesSpaceName) {
		Assert.hasText(spaceName, "Attributes space name must not be empty");
		this.attributesSpaceName = attributesSpaceName;
		this.attributesSpace = null;
	}

	public void setAttributesSpace(int attributesSpace) {
		this.attributesSpace = attributesSpace;
	}

	private int getSpace(TarantoolClient client) {
		if (space == null) {
			space = client.space(spaceName);
		}
		return space;
	}

	private int getAttributesSpace(TarantoolClient client) {
		if (attributesSpace == null) {
			attributesSpace = client.space(attributesSpaceName);
		}
		return attributesSpace;
	}

	/**
	 * Set the maximum inactive interval in seconds between requests before
	 * newly created sessions will be invalidated. A negative time indicates
	 * that the session will never timeout. The default is 1800 (30 minutes).
	 * 
	 * @param defaultMaxInactiveInterval
	 *            the maximum inactive interval in seconds
	 */
	public void setDefaultMaxInactiveInterval(Integer defaultMaxInactiveInterval) {
		this.defaultMaxInactiveInterval = defaultMaxInactiveInterval;
	}

	/**
	 * Sets the {@link ConversionService} to use.
	 * 
	 * @param conversionService
	 *            the converter to set
	 */
	public void setConversionService(ConversionService conversionService) {
		Assert.notNull(conversionService, "conversionService must not be null");
		this.conversionService = conversionService;
	}

	@Override
	public TarantoolSession createSession() {
		TarantoolSession session = new TarantoolSession();
		if (this.defaultMaxInactiveInterval != null) {
			session.setMaxInactiveInterval(Duration.ofSeconds(this.defaultMaxInactiveInterval));
		}
		return session;
	}

	@Override
	public void save(final TarantoolSession session) {
		try (TarantoolClient client = clientSource.getClient()) {
			int space = getSpace(client);
			int attributesSpace = getAttributesSpace(client);
			if (session.isNew()) {
				client.insert(space);
				client.setString(session.primaryKey);
				client.setString(session.getId());
				client.setLong(session.getCreationTime().toEpochMilli());
				client.setLong(session.getLastAccessedTime().toEpochMilli());
				client.setLong(session.getExpiryTime().toEpochMilli());
				client.setString(session.getPrincipalName());

				Set<String> attributes = session.getAttributeNames();
				if (attributes.isEmpty()) {
					client.execute().consume();
				} else {
					client.addBatch();
					for (String attribute : attributes) {
						client.insert(attributesSpace);
						client.setString(session.primaryKey);
						client.setString(attribute);
						client.setBytes(serialize(session.getAttribute(attribute)));
						client.addBatch();
					}
					client.executeBatch();
				}
			} else {
				if (session.isChanged()) {
					client.update(space, 0);
					client.setString(session.primaryKey);
					client.change(Op.ASSIGN, 2, session.getId());
					client.change(IntOp.ASSIGN, 3, session.getLastAccessedTime().toEpochMilli());
					client.change(IntOp.ASSIGN, 4, (int) session.getMaxInactiveInterval().getSeconds());
					client.change(IntOp.ASSIGN, 5, session.getExpiryTime().toEpochMilli());
					client.change(Op.ASSIGN, 6, session.getPrincipalName());
					client.execute().consume();
				}

				Map<String, Object> delta = session.getDelta();
				if (!delta.isEmpty()) {
					for (final Map.Entry<String, Object> entry : delta.entrySet()) {
						if (entry.getValue() == null) {
							client.delete(attributesSpace, 0);
							client.setString(session.primaryKey);
							client.setString(entry.getKey());
						} else {
							client.upsert(attributesSpace);
							client.setString(session.primaryKey);
							client.setString(entry.getKey());
							client.setBytes(serialize(entry.getValue()));
						}
						client.addBatch();
					}
					client.executeBatch();
				}

			}
		}
		session.clearChangeFlags();
	}

	@Override
	public TarantoolSession findById(final String id) {
		try (TarantoolClient client = clientSource.getClient()) {
			int space = getSpace(client);
			int attributesSpace = getAttributesSpace(client);

			client.select(space, 0);
			client.setString(id);
			Result result = client.execute();
			if (result.getSize() == 0) {
				return null;
			}
			result.next();

			MapSession delegate = new MapSession(id);
			String primaryKey = result.getString(0);
			delegate.setCreationTime(Instant.ofEpochMilli(result.getLong(2)));
			delegate.setLastAccessedTime(Instant.ofEpochMilli(result.getLong(3)));
			// TODO ?
			delegate.setMaxInactiveInterval(Duration.ofSeconds(result.getLong(4)));
			TarantoolSession session = new TarantoolSession(primaryKey, delegate);

			// TODO attributes
			return session;

		}
	}

	@Override
	public void deleteById(final String id) {

		// TODO
	}

	@Override
	public Map<String, TarantoolSession> findByIndexNameAndIndexValue(String indexName,
			final String indexValue) {
		if (!PRINCIPAL_NAME_INDEX_NAME.equals(indexName)) {
			return Collections.emptyMap();
		}

		Map<String, TarantoolSession> sessionMap = new HashMap<>();
		// TODO
		return sessionMap;
	}

	public void cleanUpExpiredSessions() {
		// TODO
	}

	private static GenericConversionService createDefaultConversionService() {
		GenericConversionService converter = new GenericConversionService();
		converter.addConverter(Object.class, byte[].class,
				new SerializingConverter());
		converter.addConverter(byte[].class, Object.class,
				new DeserializingConverter());
		return converter;
	}

	private byte[] serialize(Object attributeValue) {
		return (byte[]) this.conversionService.convert(attributeValue,
				TypeDescriptor.valueOf(Object.class),
				TypeDescriptor.valueOf(byte[].class));
	}

	private Object deserialize(byte[] bytes) {
		return this.conversionService.convert(bytes,
				TypeDescriptor.valueOf(byte[].class),
				TypeDescriptor.valueOf(Object.class));
	}

	static final class TarantoolSession implements Session {

		private final Session delegate;

		private final String primaryKey;

		private boolean isNew;

		private boolean changed;

		private Map<String, Object> delta = new HashMap<>();

		TarantoolSession() {
			this.delegate = new MapSession();
			this.isNew = true;
			this.primaryKey = UUID.randomUUID().toString();
		}

		TarantoolSession(String primaryKey, Session delegate) {
			Assert.notNull(primaryKey, "primaryKey cannot be null");
			Assert.notNull(delegate, "Session cannot be null");
			this.primaryKey = primaryKey;
			this.delegate = delegate;
		}

		boolean isNew() {
			return this.isNew;
		}

		boolean isChanged() {
			return this.changed;
		}

		Map<String, Object> getDelta() {
			return this.delta;
		}

		void clearChangeFlags() {
			this.isNew = false;
			this.changed = false;
			this.delta.clear();
		}

		String getPrincipalName() {
			return PRINCIPAL_NAME_RESOLVER.resolvePrincipal(this);
		}

		Instant getExpiryTime() {
			return getLastAccessedTime().plus(getMaxInactiveInterval());
		}

		@Override
		public String getId() {
			return this.delegate.getId();
		}

		@Override
		public String changeSessionId() {
			this.changed = true;
			return this.delegate.changeSessionId();
		}

		@Override
		public <T> T getAttribute(String attributeName) {
			return this.delegate.getAttribute(attributeName);
		}

		@Override
		public Set<String> getAttributeNames() {
			return this.delegate.getAttributeNames();
		}

		@Override
		public void setAttribute(String attributeName, Object attributeValue) {
			this.delegate.setAttribute(attributeName, attributeValue);
			this.delta.put(attributeName, attributeValue);
			if (PRINCIPAL_NAME_INDEX_NAME.equals(attributeName) ||
					SPRING_SECURITY_CONTEXT.equals(attributeName)) {
				this.changed = true;
			}
		}

		@Override
		public void removeAttribute(String attributeName) {
			this.delegate.removeAttribute(attributeName);
			this.delta.put(attributeName, null);
		}

		@Override
		public Instant getCreationTime() {
			return this.delegate.getCreationTime();
		}

		@Override
		public void setLastAccessedTime(Instant lastAccessedTime) {
			this.delegate.setLastAccessedTime(lastAccessedTime);
			this.changed = true;
		}

		@Override
		public Instant getLastAccessedTime() {
			return this.delegate.getLastAccessedTime();
		}

		@Override
		public void setMaxInactiveInterval(Duration interval) {
			this.delegate.setMaxInactiveInterval(interval);
			this.changed = true;
		}

		@Override
		public Duration getMaxInactiveInterval() {
			return this.delegate.getMaxInactiveInterval();
		}

		@Override
		public boolean isExpired() {
			return this.delegate.isExpired();
		}

	}

	/**
	 * Resolves the Spring Security principal name.
	 *
	 * @author Vedran Pavic
	 */
	static class PrincipalNameResolver {

		private SpelExpressionParser parser = new SpelExpressionParser();

		public String resolvePrincipal(Session session) {
			String principalName = session.getAttribute(PRINCIPAL_NAME_INDEX_NAME);
			if (principalName != null) {
				return principalName;
			}
			Object authentication = session.getAttribute(SPRING_SECURITY_CONTEXT);
			if (authentication != null) {
				Expression expression = this.parser
						.parseExpression("authentication?.name");
				return expression.getValue(authentication, String.class);
			}
			return null;
		}

	}
}
