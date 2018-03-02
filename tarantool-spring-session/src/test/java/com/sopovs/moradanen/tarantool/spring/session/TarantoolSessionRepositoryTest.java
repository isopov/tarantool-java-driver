package com.sopovs.moradanen.tarantool.spring.session;

import static com.sopovs.moradanen.tarantool.spring.session.TarantoolSessionRepository.DEFAULT_ATTRIBUTES_SPACE_NAME;
import static com.sopovs.moradanen.tarantool.spring.session.TarantoolSessionRepository.DEFAULT_SPACE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.springframework.session.FindByIndexNameSessionRepository.PRINCIPAL_NAME_INDEX_NAME;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
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
		clientSource = new TarantoolPooledClientSource("localhost", 3301, 1);
		sessionRepository = new TarantoolSessionRepository(clientSource);
		sessionRepository.createSpaces();
		session = sessionRepository.createSession();
		assertNotNull(session.getId());
	}

	@After
	public void tearDown() {
		try (TarantoolClient client = clientSource.getClient()) {
			client.evalFully("box.space." + DEFAULT_SPACE_NAME + ":drop()").consume();
			client.evalFully("box.space." + DEFAULT_ATTRIBUTES_SPACE_NAME + ":drop()").consume();
		}
		clientSource.close();
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

	@Ignore // TODO
	@Test
	public void testSaveChangedSessionId() {
		sessionRepository.save(session);
		session.changeSessionId();
		sessionRepository.save(session);
	}

	@Test
	public void testFindById() {
		sessionRepository.save(session);
		TarantoolSession got = sessionRepository.findById(session.getId());
		assertEquals(session.getId(), got.getId());
		assertEquals(session.getCreationTime(), got.getCreationTime());
		assertEquals(session.getAttributeNames(), got.getAttributeNames());
	}

	@Ignore // TODO
	@Test
	public void testFindByChangedId() {
		sessionRepository.save(session);
		String prevId = session.getId();
		session.changeSessionId();
		sessionRepository.save(session);

		assertNull(sessionRepository.findById(prevId));

		TarantoolSession got = sessionRepository.findById(session.getId());
		assertEquals(session.getId(), got.getId());
		assertEquals(session.getCreationTime(), got.getCreationTime());
		assertEquals(session.getAttributeNames(), got.getAttributeNames());
	}

}
