package com.sopovs.moradanen.tarantool;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.msgpack.core.MessageUnpacker;

public abstract class AbstractResult<T> implements Result {

	private int counter;
	protected final MessageUnpacker unpacker;
	protected T current;

	AbstractResult(MessageUnpacker unpacker) {
		this.unpacker = unpacker;
	}

	public abstract int getSize();

	public boolean hasNext() {
		return counter < getSize();
	}

	public void consume() {
		while (counter < getSize()) {
			counter++;
			try {
				// TODO seems like it may hang in case no data to read...
				unpacker.unpackValue();
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}
	}

	protected abstract T nextInternal();

	@Override
	public boolean next() {
		if (hasNext()) {
			counter++;
			current = nextInternal();
			return true;
		}
		return false;
	}

}
