package org.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;


public class HdfsUtils {
	
	private  FileSystem fs=null;
	@Before
	public void init()throws Exception{
		Configuration  conf=new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoop101:9000/");
		//模拟root用户
		 fs=FileSystem.get(new URI("hdfs://hadoop101:9000/"), conf, "root");
		
	}

	/**
	 * 上传
	 * @throws Exception
	 */
	@Test
	public void upload() throws Exception{
		fs.copyFromLocalFile(new Path("c:/1.sql"), new Path("/aa/bb/1.sql"));
	}
	@Test
	public void download()throws Exception{
		fs.copyToLocalFile(false,new Path("/aa/bb/1.sql"), new Path("c:/133.sql"),true);
		
	}
	@Test
	public void mkdir()throws Exception{
		fs.mkdirs(new Path("/cc/dd"));
	}
	
	@Test
	public void delete() throws Exception{
		fs.delete(new Path("/aa"), true);
	}
	
}
