/**
 * Verteilte und Parallele Programmierung SS 2014 Abschlussprojekt Bearbeiter:
 */
package vps.mapreduce.writer;


/**
 * Writes elements to different underlying writers
 */
public class SplittingWriter<ValueType> implements Writer<ValueType> {

	Writer<ValueType>[] writer;
	int cycler;
	
	// Constructors
	/**
	 * Creates an instance of SplittingWriter
	 * 
	 * @param p_writer
	 *            the writer to use
	 */
	public SplittingWriter(final Writer<ValueType>[] p_writer) 
	{
		this.writer = p_writer;
		this.cycler = 0;
	}

	// Methods
	/**
	 * Writes an element
	 * 
	 * @param p_element
	 *            the element
	 */
	@Override
	public void write(final ValueType p_element) 
	{
		writer[cycler].write(p_element);
		cycler = (cycler + 1) % writer.length; 
	}

	/**
	 * Closes the writer
	 */
	@Override
	public void close()
	{
		for(int i = 0; i < this.writer.length; i++)
		{
			this.writer[i].close();
		}
	}

}
