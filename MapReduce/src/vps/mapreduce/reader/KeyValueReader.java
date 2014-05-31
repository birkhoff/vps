/**
 * Verteilte und Parallele Programmierung SS 2014 Abschlussprojekt Bearbeiter:
 */
package vps.mapreduce.reader;

import vps.mapreduce.util.KeyValuePair;
import vps.mapreduce.Configuration;

/**
 * Reads a KeyValuePair from an underlying reader
 */
public class KeyValueReader implements Reader<KeyValuePair<String, String>> {

	
	int line;
	Reader<String> reader;
	
	// Constructors
	/**
	 * Creates an instance of KeyValueReader
	 * 
	 * @param p_reader
	 *            the underlying reader to use
	 */
	public KeyValueReader(final Reader<String> p_reader) 
	{	
		this.reader = p_reader;
		this.line = 1;
	}

	// Methods
	/**
	 * Reads a KeyValuePair
	 * 
	 * @return the KeyValuePair
	 */
	@Override
	public KeyValuePair<String, String> read() 
	{
		
		String current = this.reader.read();
		
		if(current == null)
			return null;		// Error
					
		String seperated[] = current.split(Configuration.KEY_VALUE_SEPARATOR);
		
		String key = seperated[0];
		String val;
		
		if(seperated.length > 1)
			val = seperated[1];
		else
			val = ""+this.line;
		
		this.line++;
		
		KeyValuePair<String, String> return_val = new KeyValuePair<String, String>(key, val);	
		//System.out.println(" key: "+ current+"\t\t val: " + val);
		return return_val;
	}

	/**
	 * Closes the reader
	 */
	@Override
	public void close() 
	{
		this.reader.close();
	}

}
