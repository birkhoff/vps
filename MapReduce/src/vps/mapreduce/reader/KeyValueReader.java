/**
 * Verteilte und Parallele Programmierung SS 2014 Abschlussprojekt Bearbeiter:
 */
package vps.mapreduce.reader;

import vps.mapreduce.util.KeyValuePair;

/**
 * Reads a KeyValuePair from an underlying reader
 */
public class KeyValueReader implements Reader<KeyValuePair<String, String>> {

	// Constructors
	/**
	 * Creates an instance of KeyValueReader
	 * 
	 * @param p_reader
	 *            the underlying reader to use
	 */
	public KeyValueReader(final Reader<String> p_reader) {
		// TODO: Aufgabe 1.1
	}

	// Methods
	/**
	 * Reads a KeyValuePair
	 * 
	 * @return the KeyValuePair
	 */
	@Override
	public KeyValuePair<String, String> read() {
		// TODO: Aufgabe 1.1
		return null;
	}

	/**
	 * Closes the reader
	 */
	@Override
	public void close() {
		// TODO: Aufgabe 1.1
	}

}
