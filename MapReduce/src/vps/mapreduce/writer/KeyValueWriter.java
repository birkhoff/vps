/**
 * Verteilte und Parallele Programmierung SS 2014 Abschlussprojekt Bearbeiter:
 */
package vps.mapreduce.writer;

import vps.mapreduce.util.KeyValuePair;
import vps.mapreduce.Configuration;

/**
 * Writes a KeyValuePair to an underlying writer
 */
public class KeyValueWriter implements Writer<KeyValuePair<String, String>> {

	
	Writer<String> writer;
	
	// Constructors
	/**
	 * Creates an instance of KeyValueWriter
	 * 
	 * @param p_writer
	 *            the underlying writer to use
	 */
	public KeyValueWriter(final Writer<String> p_writer) 
	{
		
		this.writer = p_writer;
	}

	// Methods
	/**
	 * Writes a KeyValuePair
	 * 
	 * @param p_element
	 *            the KeyValuePair
	 */
	@Override
	public void write(final KeyValuePair<String, String> p_element) 
	{
		String out = p_element.getKey() + Configuration.KEY_VALUE_SEPARATOR + p_element.getValue();
		this.writer.write(out);
	}

	/**
	 * Closes the writer
	 */
	@Override
	public void close()
	{
		this.writer.close();
	}

}
