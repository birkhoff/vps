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
		
		
		if(this.lookahead == null)
			return null;
		
		vals.clear();
		vals.add(this.lookahead.getValue());					// add Value of current lookahead

		KeyValuePair<KeyType, ValueType> pair;
		pair = this.reader.read();								// here crash
	
		while(pair != null && pair.getKey().compareTo(this.lookahead.getKey()) == 0)
		{	
			vals.add(pair.getValue());						// add Value of pair from last iteration
			pair = this.reader.read();						// set pair to the next
		}
		
		
		KeyValuePair<KeyType, Iterable<ValueType>> return_pair = new KeyValuePair<KeyType, Iterable<ValueType>>(lookahead.getKey(), vals);
		
		this.lookahead = pair;									// set lookahead to new pair
		
		
		
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
