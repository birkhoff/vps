/**
 * Verteilte und Parallele Programmierung SS 2014 Abschlussprojekt Bearbeiter:
 */
package vps.mapreduce.reader;

import vps.mapreduce.util.KeyValuePair;

/**
 * Reads a KeyValuePair from an underlying reader
 */
public class IterableValueReader<KeyType extends Comparable<KeyType>, ValueType> implements
		Reader<KeyValuePair<KeyType, Iterable<ValueType>>> {

	// Constructors
	/**
	 * Creates an instance of IterableValueReader
	 * 
	 * @param p_reader
	 *            the underlying reader to use
	 */
	public IterableValueReader(final Reader<KeyValuePair<KeyType, ValueType>> p_reader) {
		// TODO: Aufgabe 1.2
	}

	// Methods
	/**
	 * Reads a KeyValuePair
	 * 
	 * @return the KeyValuePair
	 */
	@Override
	public KeyValuePair<KeyType, Iterable<ValueType>> read() {
		// TODO: Aufgabe 1.2
		return null;
	}

	/**
	 * Closes the reader
	 */
	@Override
	public void close() {
		// TODO: Aufgabe 1.2
	}

}
