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
		return new CFMapper(p_id, p_reader, p_writer);
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
		return new CFReducer(p_id, p_reader, p_writer);
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
				
				if(!self.hasNext() && !other.hasNext())
					return 0;
				
				if(self.hasNext())
					self_x = self.next();
				else
					return -1;
				
				if(other.hasNext())
					other_y = other.next();
				else
					return 1;
				
				//System.out.println(this.m_set+" "+p_other.m_set+"\n"+self_x+"  "+other_y);
				
				
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
	
	
	private class CFMapper extends Mapper<CFSet, CFSet> {

		int id;
		// Constructors
		/**
		 * Creates an instance of CFMapper
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
			this.id = p_id;
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
		protected void map(final KeyValuePair<CFSet, CFSet> p_pair, final MapContext<CFSet, CFSet> p_context) {
			Contract.checkNotNull(p_pair, "no pair given");
			Contract.checkNotNull(p_context, "no context given");
			
			Iterator<String> val_iter = p_pair.getValue().iterator();
			
			while(val_iter.hasNext())
			{
				
				String curr_val = val_iter.next();
				
				CFSet key_pair = new CFSet();
				key_pair.add(p_pair.getKey().iterator().next());
				key_pair.add(curr_val);
								
				p_context.store(new KeyValuePair<>(key_pair, p_pair.getValue()));
				
			}
			
		}

		/**
		 * Gets a reader for KeyValuePairs
		 * 
		 * @param p_reader
		 *            the underlying reader
		 * @return a reader for KeyValuePairs
		 */
		@Override
		protected Reader<KeyValuePair<CFSet, CFSet>> getReader(final Reader<String> p_reader) {
			return new CFSetReader(p_reader);
		}

		/**
		 * Gets a writer for KeyValuePairs
		 * 
		 * @param p_writer
		 *            the underlying writer
		 * @return a writer for KeyValuePairs
		 */
		@Override
		protected Writer<KeyValuePair<CFSet, CFSet>> getWriter(final Writer<String> p_writer) {
			return new CFSetWriter(p_writer);
		}

	}
	
	/**
	 * The reducer for the word count
	 */
	private class CFReducer extends Reducer<CFSet, CFSet> {

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
		protected void reduce(final KeyValuePair<CFSet, Iterable<CFSet>> p_pair,
				final ReduceContext<CFSet, CFSet> p_context) {
			
			Contract.checkNotNull(p_pair, "no pair given");
			Contract.checkNotNull(p_context, "no context given");

			
			CFSet intersect = new CFSet();
			intersect = CFSet.intersect(p_pair.getValue());
	
			p_context.store(new KeyValuePair<CFSet, CFSet>(p_pair.getKey(), intersect));
		}

		/**
		 * Gets a reader for KeyValuePairs
		 * 
		 * @param p_reader
		 *            the underlying reader
		 * @return a reader for KeyValuePairs
		 */
		@Override
		protected Reader<KeyValuePair<CFSet, CFSet>> getReader(final Reader<String> p_reader) {
			return new CFSetReader(p_reader);
		}

		/**
		 * Gets a writer for KeyValuePairs
		 * 
		 * @param p_writer
		 *            the underlying writer
		 * @return a writer for KeyValuePairs
		 */
		@Override
		protected Writer<KeyValuePair<CFSet, CFSet>> getWriter(final Writer<String> p_writer) {
			return new CFSetWriter(p_writer);
		}
	}
	
	
	//// READER
	
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
					return null;		
							
				String seperated[] = current.split(Configuration.KEY_VALUE_SEPARATOR);
				
				CFSet key = new CFSet();
				CFSet val = new CFSet();
				
				key.add(seperated[0]);
				
				if(seperated.length > 1)
				{
					String vals[] = seperated[1].split(CFSet.ELEMENT_SEPERATOR);
					
					for(int i = 0; i < vals.length; i++)
						val.add(vals[i]);
				}
				
					
				
				KeyValuePair<CFSet, CFSet> return_val = new KeyValuePair<CFSet, CFSet>(key, val);	
				//System.out.println(" key: "+ key+"\t\t val: " + val);
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
	
	
	
	
	
	///// 			WRITER
	
	
	
	public class CFSetWriter implements Writer<KeyValuePair<CFSet, CFSet>> {

		
		Writer<String> writer;
		
		// Constructors
		/**
		 * Creates an instance of KeyValueWriter
		 * 
		 * @param p_writer
		 *            the underlying writer to use
		 */
		public CFSetWriter(final Writer<String> p_writer) 
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
		public void write(final KeyValuePair<CFSet, CFSet> p_element) 
		{			
			String friends = CFSet.parse(p_element.getKey());
			String common = CFSet.parse(p_element.getValue());
				
	
			String out = friends + Configuration.KEY_VALUE_SEPARATOR + common;
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
	

}
