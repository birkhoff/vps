/**
 * Verteilte und Parallele Programmierung SS 2014 Abschlussprojekt Bearbeiter:
 */
package vps.mapreduce.reader;

import java.util.ArrayList;
import java.util.LinkedList;

import vps.mapreduce.util.Contract;


/**
 * Reads from multiple readers
 */
public class MergingReader<Type extends Comparable<Type>> implements Reader<Type> {

	
	Reader<Type>[]	 reader;
	ArrayList<BufferedReader> buffs;
	
	// Constructors
	/**
	 * Creates an instance of MergingReader
	 * 
	 * @param p_reader
	 *            the reader to use
	 * @param p_comparator
	 *            the comparator to use
	 */
	public MergingReader(final Reader<Type>[] p_reader) 
	{
		
		this.reader = p_reader;
		
		buffs = new ArrayList<BufferedReader>();
		
		for(int i = 0; i < p_reader.length; i++)
		{
			
			BufferedReader buff = new BufferedReader(p_reader[i]);
			buffs.add(buff);
		}
	}

	// Methods
	/**
	 * Reads an element
	 * 
	 * @return the element
	 */
	@Override
	public Type read() 
	{
		
		Type min 	  = buffs.get(0).m_next;
		int min_index = 0;
		

		for(int i = 1; i < buffs.size(); i++)
		{

						
			Type current = buffs.get(i).m_next;
			
			
			if(min == null && current != null)
			{
				min = current;
				min_index = i;
			}
			else if(current != null && current.compareTo(min) <= 0)
			{
				min = current;
				min_index = i;
			}
			
			
		}


		return buffs.get(min_index).read();
	}

	/**
	 * Closes the reader
	 */
	@Override
	public void close()
	{
		for(int i = 0; i < this.reader.length; i ++)
		{
			reader[i].close();
			buffs.get(i).close();
		}
	}

	// Classes
	/**
	 * Buffers the next element of a reader
	 */
	private class BufferedReader implements Reader<Type>, Comparable<BufferedReader> {

		// Attributes
		private final Reader<Type> m_reader;
		private Type m_next;

		// Constructors
		/**
		 * Creates an instance of BufferedReader
		 * 
		 * @param p_reader
		 *            the reader to use
		 */
		private BufferedReader(final Reader<Type> p_reader) {
			Contract.checkNotNull(p_reader, "no reader given");

			m_reader = p_reader;

			read();
		}

		// Methods
		/**
		 * Reads an element
		 * 
		 * @return the element
		 */
		@Override
		public Type read() {
			final Type ret;

			ret = m_next;
			m_next = m_reader.read();

			return ret;
		}

		/**
		 * Closes the reader
		 */
		@Override
		public void close() {
			m_reader.close();
		}

		/**
		 * Compares this BufferedReader to another
		 * 
		 * @param p_other
		 *            the other BufferedReader
		 * @return the compare result
		 */
		@Override
		public int compareTo(final BufferedReader p_other) {
			final int ret;

			Contract.checkNotNull(p_other, "no other given");

			if (m_next == p_other.m_next) {
				ret = 0;
			} else if (m_next == null) {
				ret = -1;
			} else if (p_other.m_next == null) {
				ret = 1;
			} else {
				ret = m_next.compareTo(p_other.m_next);
			}

			return ret;
		}

		/**
		 * Gets the string representation
		 * 
		 * @return the string representation
		 */
		@Override
		public String toString() {
			return BufferedReader.class.getSimpleName() + " [m_next=" + m_next + "]";
		}

	}

}
