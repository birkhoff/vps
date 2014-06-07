/**
 * Verteilte und Parallele Programmierung SS 2014 Abschlussprojekt Bearbeiter:
 */
package vps.mapreduce.jobs;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import vps.mapreduce.Configuration;
import vps.mapreduce.core.Job;
import vps.mapreduce.core.MapContext;
import vps.mapreduce.core.Mapper;
import vps.mapreduce.core.ReduceContext;
import vps.mapreduce.core.Reducer;
import vps.mapreduce.reader.KeyValueReader;
import vps.mapreduce.reader.Reader;
import vps.mapreduce.util.Contract;
import vps.mapreduce.util.KeyValuePair;
import vps.mapreduce.writer.KeyValueWriter;
import vps.mapreduce.writer.Writer;

/**
 * Finds common friends
 */
public class CommonFriends implements Job {

	// Constructors
	/**
	 * Creates an instance of CommonFriends
	 */
	public CommonFriends() {}

	// Methods
	/**
	 * Creates a mapper
	 * 
	 * @param p_id
	 *            the id of the mapper
	 * @param p_reader
	 *            the underlying reader to use
	 * @param p_writer
	 *            the underlying writer to use
	 * @return a mapper
	 */
	@Override
	public Mapper<?, ?> createMapper(final int p_id, final Reader<String> p_reader, final Writer<String> p_writer) {
		// TODO: Aufgabe 5.2
		return null;
	}

	/**
	 * Creates a reducer
	 * 
	 * @param p_id
	 *            the id of the mapper
	 * @param p_reader
	 *            the underlying reader to use
	 * @param p_writer
	 *            the underlying writer to use
	 * @return a reducer
	 */
	@Override
	public Reducer<?, ?> createReducer(final int p_id, final Reader<String>[] p_reader, final Writer<String> p_writer) {
		// TODO: Aufgabe 5.3
		return null;
	}

	// Classes
	/**
	 * Represents a set of elements
	 */
	private static class CFSet implements Comparable<CFSet>, Iterable<String> {

		// Constants
		private static final String ELEMENT_SEPERATOR = ";";

		// Attributes
		private final Set<String> m_set;

		// Constructors
		/**
		 * Creates an instance of CFSet
		 */
		private CFSet() {
			m_set = new TreeSet<>();
		}

		/**
		 * Creates an instance of CFSet
		 * 
		 * @param p_set
		 *            the set to add
		 * @param p_element
		 *            the element to add
		 */
		private CFSet(final CFSet p_set, final String p_element) {
			m_set = new TreeSet<>(p_set.m_set);
			m_set.add(p_element);
		}

		// Methods
		/**
		 * Adds an element
		 * 
		 * @param p_element
		 *            the element to add
		 */
		public void add(final String p_element) {
			m_set.add(p_element);
		}

		/**
		 * Calculates the intersection of the given sets
		 * 
		 * @param p_sets
		 *            the sets
		 * @return the intersection
		 */
		public static CFSet intersect(final Iterable<CFSet> p_sets) {
			CFSet ret;
			Iterator<CFSet> iterator;

			ret = new CFSet();
			if (p_sets != null) {
				iterator = p_sets.iterator();

				if (iterator.hasNext()) {
					ret.m_set.addAll(iterator.next().m_set);

					while (iterator.hasNext()) {
						ret.m_set.retainAll(iterator.next().m_set);
					}
				}
			}

			return ret;
		}

		/**
		 * Parses a String
		 * 
		 * @param p_string
		 *            the String
		 * @return a CFSet
		 */
		public static CFSet parse(final String p_string) {
			CFSet ret;
			String[] elements;

			ret = new CFSet();

			elements = p_string.split(ELEMENT_SEPERATOR);
			for (final String element : elements) {
				ret.add(element);
			}

			return ret;
		}

		/**
		 * Parses a CFSet
		 * 
		 * @param p_set
		 *            the CFSet
		 * @return a string
		 */
		public static String parse(final CFSet p_set) {
			StringBuffer ret;

			ret = new StringBuffer();

			for (final String element : p_set) {
				ret.append(element + ELEMENT_SEPERATOR);
			}
			ret.deleteCharAt(ret.length() - 1);

			return ret.toString();
		}

		/**
		 * Gets the iterator
		 * 
		 * @return the iterator
		 */
		@Override
		public Iterator<String> iterator() {
			return m_set.iterator();
		}

		/**
		 * Compares this CFSet with another
		 * 
		 * @param p_other
		 *            the other CFSet
		 * @return the compare result
		 */
		@Override
		public int compareTo(final CFSet p_other) {
			
			// fix
			
			Iterator<String> self = this.m_set.iterator();
			Iterator<String> other = p_other.m_set.iterator();
			
			String self_x 	= self.next();
			String other_y 	= other.next();
			
			int diff = self_x.compareTo(other_y);
			
			while(diff == 0)
			{
				self_x = self.next();
				other_y = other.next();
				
				if(self_x == null && other_y == null)	return 0;
				if(self_x == null)						return -1;
				if(other_y == null)						return 1;
				
				diff = self_x.compareTo(other_y);
			}
			
			return diff;
		}

		/**
		 * Gets the string representation
		 * 
		 * @param the
		 *            string representation
		 */
		@Override
		public String toString() {
			return CFSet.class.getSimpleName() + " [m_set=" + Arrays.toString(m_set.toArray()) + "]";
		}

	}
	
	
	private class CFMapper extends Mapper<String, String> {

		// Constructors
		/**
		 * Creates an instance of WCMapper
		 * 
		 * @param p_id
		 *            the id of the mapper
		 * @param p_reader
		 *            the underlying reader to use
		 * @param p_writer
		 *            the underlying writer to use
		 */
		public CFMapper(final int p_id, final Reader<String> p_reader, final Writer<String> p_writer) {
			super(p_id, p_reader, p_writer);
		}

		// Methods
		/**
		 * Perform the map operation
		 * 
		 * @param p_pair
		 *            the KeyValuePair
		 * @param p_context
		 *            the context
		 */
		@Override
		protected void map(final KeyValuePair<String, String> p_pair, final MapContext<String, String> p_context) {
			Contract.checkNotNull(p_pair, "no pair given");
			Contract.checkNotNull(p_context, "no context given");

			p_context.store(new KeyValuePair<>(p_pair.getValue(), Integer.toString(1)));
		}

		/**
		 * Gets a reader for KeyValuePairs
		 * 
		 * @param p_reader
		 *            the underlying reader
		 * @return a reader for KeyValuePairs
		 */
		@Override
		protected Reader<KeyValuePair<String, String>> getReader(final Reader<String> p_reader) {
			return new KeyValueReader(p_reader);
		}

		/**
		 * Gets a writer for KeyValuePairs
		 * 
		 * @param p_writer
		 *            the underlying writer
		 * @return a writer for KeyValuePairs
		 */
		@Override
		protected Writer<KeyValuePair<String, String>> getWriter(final Writer<String> p_writer) {
			return new KeyValueWriter(p_writer);
		}

	}
	
	/**
	 * The reducer for the word count
	 */
	private class CFReducer extends Reducer<String, String> {

		// Constructors
		/**
		 * Creates an instance of WCReducer
		 * 
		 * @param p_id
		 *            the id of the mapper
		 * @param p_reader
		 *            the underlying reader to use
		 * @param p_writer
		 *            the underlying writer to use
		 */
		public CFReducer(final int p_id, final Reader<String>[] p_reader, final Writer<String> p_writer) {
			super(p_id, p_reader, p_writer);
		}

		// Methods
		/**
		 * Performs the reduce operation
		 * 
		 * @param p_pair
		 *            the KeyValuePair
		 * @param p_context
		 *            the context
		 */
		@Override
		protected void reduce(final KeyValuePair<String, Iterable<String>> p_pair,
				final ReduceContext<String, String> p_context) {
			int count;
			final Iterator<String> iterator;

			Contract.checkNotNull(p_pair, "no pair given");
			Contract.checkNotNull(p_context, "no context given");

			count = 0;
			iterator = p_pair.getValue().iterator();
			while (iterator.hasNext()) {
				iterator.next();

				count++;
			}

			p_context.store(new KeyValuePair<String, String>(p_pair.getKey(), Integer.toString(count)));
		}

		/**
		 * Gets a reader for KeyValuePairs
		 * 
		 * @param p_reader
		 *            the underlying reader
		 * @return a reader for KeyValuePairs
		 */
		@Override
		protected Reader<KeyValuePair<String, String>> getReader(final Reader<String> p_reader) {
			return new KeyValueReader(p_reader);
		}

		/**
		 * Gets a writer for KeyValuePairs
		 * 
		 * @param p_writer
		 *            the underlying writer
		 * @return a writer for KeyValuePairs
		 */
		@Override
		protected Writer<KeyValuePair<String, String>> getWriter(final Writer<String> p_writer) {
			return new KeyValueWriter(p_writer);
		}
	}
	
	
	public class CFSetReader implements Reader<KeyValuePair<CFSet, CFSet>> {
		
		Reader<String> reader;
			
			// Constructors
			/**
			 * Creates an instance of KeyValueReader
			 * 
			 * @param p_reader
			 *            the underlying reader to use
			 */
			public CFSetReader(final Reader<String> p_reader) 
			{	
				this.reader = p_reader;
			}

			// Methods
			/**
			 * Reads a KeyValuePair
			 * 
			 * @return the KeyValuePair
			 */
			@Override
			public KeyValuePair<CFSet, CFSet> read() 
			{
				
				String current = this.reader.read();
				
				if(current == null)
					return null;		// Error
							
				String seperated[] = current.split(Configuration.KEY_VALUE_SEPARATOR);
				
				CFSet key = new CFSet();
				CFSet val = new CFSet();
				
				key.add(seperated[0]);
				
				for(int i = 1; i < seperated.length; i++)
					val.add(seperated[1]);
					
				
				KeyValuePair<CFSet, CFSet> return_val = new KeyValuePair<CFSet, CFSet>(key, val);	
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

}
