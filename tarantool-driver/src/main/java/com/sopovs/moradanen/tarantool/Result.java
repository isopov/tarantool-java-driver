package com.sopovs.moradanen.tarantool;

//TODO Closeable?
public interface Result {

	public int getSize();

	public boolean hasNext();

	public boolean isNull(int index);

	public boolean getBoolean(int index);

	public double getDouble(int index) ;

	public float getFloat(int index);

	public long getLong(int index);

	public int getInt(int index) ;

	public String getString(int index) ;

	public int currentSize() ;

	public boolean next();

	public void consume();

}
