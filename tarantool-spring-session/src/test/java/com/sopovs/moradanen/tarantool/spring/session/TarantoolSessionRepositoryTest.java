package com.sopovs.moradanen.tarantool.spring.session;

import static com.sopovs.moradanen.tarantool.spring.session.TarantoolSessionRepository.ATTR_PRIMARY_INDEX;
import static com.sopovs.moradanen.tarantool.spring.session.TarantoolSessionRepository.DEFAULT_ATTRIBUTES_SPACE_NAME;
import static com.sopovs.moradanen.tarantool.spring.session.TarantoolSessionRepository.DEFAULT_SPACE_NAME;
import static com.sopovs.moradanen.tarantool.test.TestUtil.getEnvTarantoolVersion;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.springframework.session.FindByIndexNameSessionRepository.PRINCIPAL_NAME_INDEX_NAME;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.sopovs.moradanen.tarantool.TarantoolClient;
import com.sopovs.moradanen.tarantool.TarantoolClientSource;
import com.sopovs.moradanen.tarantool.TarantoolPooledClientSource;
import com.sopovs.moradanen.tarantool.spring.session.TarantoolSessionRepository.TarantoolSession;

public class TarantoolSessionRepositoryTest {

	private TarantoolClientSource clientSource;
	private TarantoolSessionRepository sessionRepository;
	private TarantoolSession session;

	@Before
	public void setUp() {
		Assume.assumeFalse(getEnvTarantoolVersion().startsWith("1.6"));
		clientSource = new TarantoolPooledClientSource("localhost", 3301, 1);
		sessionRepository = new TarantoolSessionRepository(clientSource);
		sessionRepository.createSpaces();
		session = sessionRepository.createSession();
		assertNotNull(session.getId());
	}

	@After
	public void tearDown() {
		if (clientSource == null) {
			return;
		}
		try (TarantoolClient client = clientSource.getClient()) {
			client.evalFully("box.space." + DEFAULT_SPACE_NAME + ":drop()").consume();
			client.evalFully("box.space." + DEFAULT_ATTRIBUTES_SPACE_NAME + ":drop()").consume();
		}
		clientSource.close();
	}

	@Test
	public void testCreateSpacesIfNotExist() {
		sessionRepository.createSpaces();
	}

	@Test
	public void testSaveEmpty() {
		sessionRepository.save(session);
	}

	@Test
	public void testDoubleSaveEmpty() {
		sessionRepository.save(session);
		sessionRepository.save(session);
	}

	@Test
	public void testSaveWithPrincipalName() {
		session.setAttribute(PRINCIPAL_NAME_INDEX_NAME, "foobar");
		sessionRepository.save(session);
	}

	@Test
	public void testDoubleSaveWithPrincipalName() {
		testSaveWithPrincipalName();
		sessionRepository.save(session);
	}

	@Test
	public void testSaveChangedSessionId() {
		sessionRepository.save(session);
		session.changeSessionId();
		sessionRepository.save(session);
	}

	@Test
	public void testFindById() {
		sessionRepository.save(session);
		assertSessionEquals(sessionRepository.findById(session.getId()));
	}

	@Test
	public void testFindByIdWithPrincipalName() {
		testSaveWithPrincipalName();
		TarantoolSession got = sessionRepository.findById(session.getId());
		assertSessionEquals(got);
		assertEquals("foobar", got.getPrincipalName());
		assertEquals(1, got.getAttributeNames().size());
	}

	@Test
	public void testDeleteByIdNotSaved() {
		sessionRepository.deleteById(session.getId());
	}

	@Test
	public void testDeleteById() {
		sessionRepository.save(session);
		sessionRepository.deleteById(session.getId());
		assertNull(sessionRepository.findById(session.getId()));
	}

	@Test
	public void testDeleteByIdWIthAttribute() {
		session.setAttribute("foo", "bar");
		sessionRepository.save(session);
		sessionRepository.deleteById(session.getId());
		assertNull(sessionRepository.findById(session.getId()));

		try (TarantoolClient client = clientSource.getClient()) {
			client.select(sessionRepository.getAttributesSpace(client), ATTR_PRIMARY_INDEX);
			client.setLong(session.primaryKey.getMostSignificantBits());
			client.setLong(session.primaryKey.getLeastSignificantBits());
			assertEquals(0, client.execute().getSize());
		}
	}

	@Test
	public void testRemoveAttribute() {
		session.setAttribute("foo", "bar");
		sessionRepository.save(session);
		assertSessionEquals(sessionRepository.findById(session.getId()));

		session.removeAttribute("foo");
		sessionRepository.save(session);
		TarantoolSession got = sessionRepository.findById(session.getId());
		assertSessionEquals(got);
		assertEquals(0, got.getAttributeNames().size());
	}

	@Test
	public void testFindByNewlyChangedId() {
		session.changeSessionId();
		sessionRepository.save(session);
		assertSessionEquals(sessionRepository.findById(session.getId()));
	}

	@Test
	public void testFindByChangedId() {
		sessionRepository.save(session);
		String prevId = session.getId();
		session.changeSessionId();
		sessionRepository.save(session);

		assertNull(sessionRepository.findById(prevId));
		assertSessionEquals(sessionRepository.findById(session.getId()));
	}

	private void assertSessionEquals(TarantoolSession got) {
		assertSessionEquals(session, got);
	}

	private void assertSessionEquals(TarantoolSession expected, TarantoolSession actual) {
		assertEquals(expected.primaryKey, actual.primaryKey);
		assertEquals(expected.getId(), actual.getId());
		assertEquals(expected.getLastAccessedTime().toEpochMilli(), actual.getLastAccessedTime().toEpochMilli());
		assertEquals(expected.getMaxInactiveInterval(), actual.getMaxInactiveInterval());
		assertEquals(expected.getExpiryTime().toEpochMilli(), actual.getExpiryTime().toEpochMilli());
		assertEquals(expected.getCreationTime().toEpochMilli(), actual.getCreationTime().toEpochMilli());
		assertEquals(expected.getAttributeNames(), actual.getAttributeNames());
		assertEquals(expected.getPrincipalName(), actual.getPrincipalName());
	}

	@Test
	public void testFindByIndexNameAndIndexValue() {
		testSaveWithPrincipalName();
		Map<String, TarantoolSession> result = sessionRepository.findByIndexNameAndIndexValue(PRINCIPAL_NAME_INDEX_NAME,
				"foobar");
		assertEquals(1, result.size());
		assertSessionEquals(result.get(session.getId()));
	}

	@Test
	public void testFind2ByIndexNameAndIndexValue() {
		testSaveWithPrincipalName();

		TarantoolSession session2 = sessionRepository.createSession();
		session2.setAttribute(PRINCIPAL_NAME_INDEX_NAME, "foobar");
		sessionRepository.save(session2);

		Map<String, TarantoolSession> result = sessionRepository.findByIndexNameAndIndexValue(PRINCIPAL_NAME_INDEX_NAME,
				"foobar");
		assertEquals(2, result.size());
		assertSessionEquals(result.get(session.getId()));
		assertSessionEquals(session2, result.get(session2.getId()));
	}

	@Test
	public void testEqualEpirations() {
		TarantoolSession session2 = sessionRepository.createSession();
		Instant now = Instant.now();
		session.setLastAccessedTime(now);
		session2.setLastAccessedTime(now);
		sessionRepository.save(session);
		sessionRepository.save(session2);

		assertEquals(sessionRepository.findById(session.getId()).getExpiryTime(),
				sessionRepository.findById(session2.getId()).getExpiryTime());
	}

	@Test
	public void testCleanUpExpiredSessions() {
		session.setLastAccessedTime(Instant.now().minus(Duration.ofDays(100)));
		sessionRepository.save(session);
		assertSessionEquals(sessionRepository.findById(session.getId()));
		sessionRepository.cleanUpExpiredSessions();
		assertNull(sessionRepository.findById(session.getId()));
	}

}
