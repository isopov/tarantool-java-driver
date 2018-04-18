package com.sopovs.moradanen.tarantool.spring.session;

import com.sopovs.moradanen.tarantool.Result;
import com.sopovs.moradanen.tarantool.TarantoolClient;
import com.sopovs.moradanen.tarantool.TarantoolClientSource;
import com.sopovs.moradanen.tarantool.core.IntOp;
import com.sopovs.moradanen.tarantool.core.Iter;
import com.sopovs.moradanen.tarantool.core.Op;
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

import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class TarantoolSessionRepository
        implements FindByIndexNameSessionRepository<TarantoolSessionRepository.TarantoolSession> {

    private static final int SPACE_PRIMARY_INDEX = 0;
    private static final int SPACE_ID_INDEX = 1;
    private static final int SPACE_NAME_INDEX = 2;
    private static final int SPACE_EXPIRY_INDEX = 3;
    static final int ATTR_PRIMARY_INDEX = 0;
    public static final String DEFAULT_SPACE_NAME = "spring_session";
    public static final String DEFAULT_ATTRIBUTES_SPACE_NAME = DEFAULT_SPACE_NAME + "_attrs";

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

    private ConversionService conversionService = createDefaultConversionService();

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

    int getAttributesSpace(TarantoolClient client) {
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
     * @param defaultMaxInactiveInterval the maximum inactive interval in seconds
     */
    public void setDefaultMaxInactiveInterval(Integer defaultMaxInactiveInterval) {
        this.defaultMaxInactiveInterval = defaultMaxInactiveInterval;
    }

    /**
     * Sets the {@link ConversionService} to use.
     *
     * @param conversionService the converter to set
     */
    void setConversionService(ConversionService conversionService) {
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
                client.setLong(session.primaryKey.getMostSignificantBits());
                client.setLong(session.primaryKey.getLeastSignificantBits());
                client.setLong(session.id.getMostSignificantBits());
                client.setLong(session.id.getLeastSignificantBits());
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
                        client.setLong(session.primaryKey.getMostSignificantBits());
                        client.setLong(session.primaryKey.getLeastSignificantBits());
                        client.setString(attribute);
                        client.setBytes(serialize(session.getAttribute(attribute)));
                        client.addBatch();
                    }
                    client.executeBatch();
                }
            } else {
                if (session.isChanged()) {
                    client.update(space, SPACE_PRIMARY_INDEX);
                    client.setLong(session.primaryKey.getMostSignificantBits());
                    client.setLong(session.primaryKey.getLeastSignificantBits());
                    client.change(IntOp.ASSIGN, 2, session.id.getMostSignificantBits());
                    client.change(IntOp.ASSIGN, 3, session.id.getLeastSignificantBits());
                    client.change(IntOp.ASSIGN, 5, session.getLastAccessedTime().toEpochMilli());
                    client.change(IntOp.ASSIGN, 6, session.getExpiryTime().toEpochMilli());
                    client.change(Op.ASSIGN, 7, session.getPrincipalName());
                    client.execute().consume();
                }

                Map<String, Object> delta = session.getDelta();
                if (!delta.isEmpty()) {
                    for (final Map.Entry<String, Object> entry : delta.entrySet()) {
                        if (entry.getValue() == null) {
                            client.delete(attributesSpace, ATTR_PRIMARY_INDEX);
                            client.setLong(session.primaryKey.getMostSignificantBits());
                            client.setLong(session.primaryKey.getLeastSignificantBits());
                            client.setString(entry.getKey());
                        } else {
                            client.upsert(attributesSpace);
                            client.setLong(session.primaryKey.getMostSignificantBits());
                            client.setLong(session.primaryKey.getLeastSignificantBits());
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
            return findById(UUID.fromString(id), id, client);
        }
    }

    @Override
    public void deleteById(final String id) {
        // TODO lua-procedure
        try (TarantoolClient client = clientSource.getClient()) {
            deleteById(UUID.fromString(id), id, client);
        }
    }

    private void deleteById(UUID id, TarantoolClient client) {
        deleteById(id, id.toString(), client);
    }

    private void deleteById(UUID uuid, String id, TarantoolClient client) {
        int space = getSpace(client);
        TarantoolSession session = findById(uuid, id, client);
        if (session == null) {
            return;
        }
        client.delete(space, SPACE_PRIMARY_INDEX);
        client.setLong(session.primaryKey.getMostSignificantBits());
        client.setLong(session.primaryKey.getLeastSignificantBits());
        Set<String> attributeNames = session.getAttributeNames();
        if (attributeNames.isEmpty()) {
            client.execute().consume();
        } else {
            client.addBatch();

            int attributesSpace = getAttributesSpace(client);
            for (String attribute : attributeNames) {
                client.delete(attributesSpace, ATTR_PRIMARY_INDEX);
                client.setLong(session.primaryKey.getMostSignificantBits());
                client.setLong(session.primaryKey.getLeastSignificantBits());
                client.setString(attribute);
                client.addBatch();
            }
            client.executeBatch();
        }
    }

    private TarantoolSession findById(UUID uuid, String id, TarantoolClient client) {
        int space = getSpace(client);
        client.select(space, SPACE_ID_INDEX);
        client.setLong(uuid.getMostSignificantBits());
        client.setLong(uuid.getLeastSignificantBits());
        Result result = client.execute();
        if (result.getSize() == 0) {
            return null;
        }
        assert result.getSize() == 1;
        result.next();

        TarantoolSession session = getSession(uuid, id, result);
        getAttributes(session, client);
        return session;
    }

    private TarantoolSession getSession(UUID uuid, String id, Result result) {
        MapSession delegate = new MapSession(id);
        delegate.setCreationTime(Instant.ofEpochMilli(result.getLong(4)));
        delegate.setLastAccessedTime(Instant.ofEpochMilli(result.getLong(5)));

        return new TarantoolSession(
                new UUID(result.getLong(0), result.getLong(1)),
                uuid, delegate);
    }

    private void getAttributes(TarantoolSession session, TarantoolClient client) {
        int attributesSpace = getAttributesSpace(client);
        client.select(attributesSpace, ATTR_PRIMARY_INDEX);
        client.setLong(session.primaryKey.getMostSignificantBits());
        client.setLong(session.primaryKey.getLeastSignificantBits());
        Result result = client.execute();
        while (result.next()) {
            session.setAttribute(result.getString(2), deserialize(result.getBytes(3)));
        }
    }

    @Override
    public Map<String, TarantoolSession> findByIndexNameAndIndexValue(String indexName, final String indexValue) {
        if (!PRINCIPAL_NAME_INDEX_NAME.equals(indexName)) {
            return Collections.emptyMap();
        }

        Map<String, TarantoolSession> sessionMap = new HashMap<>();

        // TODO N+1 queries - lua procedure for the rescue
        try (TarantoolClient client = clientSource.getClient()) {
            client.select(getSpace(client), SPACE_NAME_INDEX);
            client.setString(indexValue);
            Result result = client.execute();
            while (result.next()) {
                UUID uuid = new UUID(result.getLong(2), result.getLong(3));
                sessionMap.put(uuid.toString(), getSession(uuid, uuid.toString(), result));
            }

            for (TarantoolSession session : sessionMap.values()) {
                getAttributes(session, client);
            }
        }
        return sessionMap;
    }

    void cleanUpExpiredSessions() {
        // TODO another lua procedure
        try (TarantoolClient client = clientSource.getClient()) {
            client.select(getSpace(client), SPACE_EXPIRY_INDEX, Integer.MAX_VALUE, 0, Iter.LE);
            client.setLong(System.currentTimeMillis());
            Result result = client.execute();
            Set<UUID> ids = new HashSet<>();
            while (result.next()) {
                ids.add(new UUID(result.getLong(2), result.getLong(3)));
            }
            ids.forEach(id -> deleteById(id, client));
            if (logger.isDebugEnabled()) {
                logger.debug("Cleaned up " + ids.size() + " expired sessions");
            }
        }
    }

    private static GenericConversionService createDefaultConversionService() {
        GenericConversionService converter = new GenericConversionService();
        converter.addConverter(Object.class, byte[].class, new SerializingConverter());
        converter.addConverter(byte[].class, Object.class, new DeserializingConverter());
        return converter;
    }

    private byte[] serialize(Object attributeValue) {
        return (byte[]) this.conversionService.convert(attributeValue, TypeDescriptor.valueOf(Object.class),
                TypeDescriptor.valueOf(byte[].class));
    }

    private Object deserialize(byte[] bytes) {
        return this.conversionService.convert(bytes, TypeDescriptor.valueOf(byte[].class),
                TypeDescriptor.valueOf(Object.class));
    }

    static final class TarantoolSession implements Session {

        private final MapSession delegate;

        final UUID primaryKey;

        private UUID id;

        private boolean isNew;

        private boolean changed;

        private final Map<String, Object> delta = new HashMap<>();

        TarantoolSession() {
            this.id = UUID.randomUUID();
            this.delegate = new MapSession(id.toString());
            this.isNew = true;
            this.primaryKey = UUID.randomUUID();
        }

        TarantoolSession(UUID primaryKey, UUID id, MapSession delegate) {
            Assert.notNull(primaryKey, "primaryKey cannot be null");
            Assert.notNull(id, "Id cannot be null");
            Assert.notNull(delegate, "Session cannot be null");
            this.primaryKey = primaryKey;
            this.id = id;
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
            this.id = UUID.randomUUID();
            String changed = id.toString();
            this.delegate.setId(changed);
            return changed;
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
            if (PRINCIPAL_NAME_INDEX_NAME.equals(attributeName) || SPRING_SECURITY_CONTEXT.equals(attributeName)) {
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

        private final SpelExpressionParser parser = new SpelExpressionParser();

        String resolvePrincipal(Session session) {
            String principalName = session.getAttribute(PRINCIPAL_NAME_INDEX_NAME);
            if (principalName != null) {
                return principalName;
            }
            Object authentication = session.getAttribute(SPRING_SECURITY_CONTEXT);
            if (authentication != null) {
                Expression expression = this.parser.parseExpression("authentication?.name");
                return expression.getValue(authentication, String.class);
            }
            return null;
        }

    }

    public void createSpaces() {
        try (TarantoolClient client = clientSource.getClient()) {
            createSpaces(spaceName, attributesSpaceName, client);
        }
    }

    private static void createSpaces(String spaceName, String attributesSpaceName, TarantoolClient client) {
        client.evalFully("box.schema.space.create('" + spaceName + "', {if_not_exists=true})").consume();
        // TODO format for field count and types
        // SPACE_PRIMARY_INDEX
        client.evalFully(
                "box.space." + spaceName
                        + ":create_index('primary', {type = 'hash', if_not_exists=true, parts = {{1, 'int'}, {2, 'int'}}})")
                .consume();
        // SPACE_ID_INDEX
        client.evalFully(
                "box.space." + spaceName
                        + ":create_index('id', {type = 'hash', if_not_exists=true, parts = {{3, 'int'}, {4, 'int'}}})")
                .consume();
        // SPACE_NAME_INDEX
        client.evalFully("box.space." + spaceName
                + ":create_index('name', {type = 'tree', if_not_exists=true, parts = {{8, 'string', is_nullable=true}}, unique=false})")
                .consume();
        // SPACE_EXPIRY_INDEX
        client.evalFully("box.space." + spaceName
                + ":create_index('expiry', {type = 'tree', if_not_exists=true, parts = {{7, 'uint'}}, unique=false})")
                .consume();

        client.evalFully("box.schema.space.create('" + attributesSpaceName + "', {if_not_exists=true})").consume();
        client.evalFully("box.space." + attributesSpaceName
                + ":create_index('primary', {type = 'tree', if_not_exists=true, parts = {{1, 'int'}, {2, 'int'}, {3, 'string'}}})")
                .consume();
    }

}
