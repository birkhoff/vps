/**
 * Verteilte und Parallele Programmierung SS 2014 Abschlussprojekt Bearbeiter:
 */
package vps.mapreduce;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import vps.mapreduce.core.Job;
import vps.mapreduce.core.Mapper;
import vps.mapreduce.core.Reducer;
import vps.mapreduce.jobs.*;
import vps.mapreduce.reader.Reader;
import vps.mapreduce.util.Executor;
import vps.mapreduce.writer.Writer;
import vps.mapreduce.Configuration;

/**
 * Start class for MapReduce
 */
public class MapReduce {

	// Constructors
	/**
	 * Creates an instance of MapReduce
	 */
	private MapReduce() {}

	// javac vps/mapreduce/*.java vps/mapreduce/**/**.java
	// java vps.mapreduce.MapReduce <job> <infile> <temp_prefix> <out_prefix>
	// java  vps.mapreduce.MapReduce WordCount ../example_wc.txt test out2.txt
	
	// Methods
	/**
	 * Programm entry point for MapReduce
	 * 
	 * @param p_arguments
	 *            the programm arguments
	 */
	public static void main(final String[] p_arguments)
	{	
		Job job 		= instantiate_job(p_arguments[0]);
		String infile	= p_arguments[1];					// input file
		String temp		= p_arguments[2];					// temp file
		String out		= p_arguments[3];					// output file
		
		
		try
		{
			start(job, infile, out, temp);
		
		} catch (IOException e) {
			System.out.println("Starting failed");
			e.printStackTrace();
		}
		
				
	}
	
	
	
	private static void start(Job job, String infile, String out, String temp) throws IOException
	{
			
		Mapper[] mapper = new Mapper[Configuration.MAPPER_COUNT];
		Reducer[] reducer = new Reducer[Configuration.REDUCER_COUNT];
		
		int curr_lines = Configuration.getLineCount(infile);
		int curr_map   = Configuration.MAPPER_COUNT;
		
		System.out.println("Mapper: "+ curr_map + "\nLines: "+ curr_lines);
		
		int offs =  (int) Math.ceil((double)curr_lines/curr_map);
		
		System.out.println("Offsets: "+ offs);
		
		int p_id;
		for(p_id = 0; p_id < curr_map; p_id ++)
		{
			int read_lines = offs;
			if( (p_id*offs+offs) > curr_lines)
				read_lines = (curr_lines - (p_id*offs));
			//System.out.println(read_lines);
			//System.out.println("Mapper starts: "+p_id*offs+" and reads: " + read_lines+"   = " + ((p_id*offs)+read_lines));
			
			Reader<String> 	m_r = Configuration.createMapperReader(infile, p_id*offs, read_lines);
			Writer<String> 	m_w = Configuration.createMapperWriter(temp, p_id); 
			
			mapper[p_id] = job.createMapper(p_id, m_r, m_w);
		}
		
		Executor.execute(mapper, Configuration.THREAD_COUNT);
		
				
		System.out.println("p_id: "+p_id+"\nReducer: " + Configuration.REDUCER_COUNT);
		
		
		for(p_id = 0; p_id < Configuration.REDUCER_COUNT; p_id ++)
		{
			System.out.println("Reducer_"+p_id);
			Reader<String>[] 	r_r = Configuration.createReducerReader(temp, p_id);
			Writer<String> 		r_w = Configuration.createReducerWriter(out, p_id);
			
			reducer[p_id] = job.createReducer(p_id, r_r, r_w);
		}
		
		
		
		Executor.execute(reducer, Configuration.THREAD_COUNT);
	}
	
	private static Job instantiate_job(final String job_name)
	{
		Job job = null;
		
		try 
		{
			//job = (Job)Class.forName("vps.mapreduce.jobs."+job_name).newInstance();
			
			Class<?> myClass = Class.forName("vps.mapreduce.jobs."+job_name);

			Constructor<?> constructor = myClass.getConstructor();

			Object[] parameters = {};
			job = (Job)constructor.newInstance(parameters);
		
		} catch (ClassNotFoundException e) {
			System.out.println("Class not found");
			e.printStackTrace();
				
		} catch (InstantiationException e) {
			System.out.println("Could not instanticate Object");
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			System.out.println("Da lief grundsaetzlich was schief!");
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	
		return job;
	}

}
