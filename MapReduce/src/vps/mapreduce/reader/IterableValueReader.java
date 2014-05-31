/**
 * Verteilte und Parallele Programmierung SS 2014 Abschlussprojekt Bearbeiter:
 */
package vps.mapreduce.reader;

import java.util.LinkedList;

import vps.mapreduce.util.KeyValuePair;

/**
 * Reads a KeyValuePair from an underlying reader
 */
public class IterableValueReader<KeyType extends Comparable<KeyType>, ValueType> implements
		Reader<KeyValuePair<KeyType, Iterable<ValueType>>> {

	Reader<KeyValuePair<KeyType, ValueType>> 		reader;
	LinkedList<ValueType>   						vals;
	KeyValuePair<KeyType, ValueType>				lookahead;
	
	// Constructors
	/**
	 * Creates an instance of IterableValueReader
	 * 
	 * @param p_reader
	 *            the underlying reader to use
	 */
	public IterableValueReader(final Reader<KeyValuePair<KeyType, ValueType>> p_reader)
	{
		this.reader 	= p_reader;
		this.vals 		= new LinkedList<ValueType>();
		this.lookahead	= this.reader.read();				// possible error
	}

	// Methods
	/**
	 * Reads a KeyValuePair
	 * 
	 * @return the KeyValuePair
	 */
	// possible error with lookahead
	@Override
	public KeyValuePair<KeyType, Iterable<ValueType>> read() {
		
		vals.clear();
		vals.add(lookahead.getValue());					// add Value of current lookahead
		
		KeyValuePair<KeyType, ValueType> pair;
		pair = this.reader.read();
		
		while(pair != null && pair.getKey() == lookahead.getKey())
		{	
			
			vals.add(pair.getValue());
			pair = this.reader.read();
		}
		
		KeyValuePair<KeyType, Iterable<ValueType>> return_pair = new KeyValuePair<KeyType, Iterable<ValueType>>(lookahead.getKey(), vals);
		
		if(pair != null)	lookahead = pair;			// set lookahead to new pair
		
		return return_pair;
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
