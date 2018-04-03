package org.apache.nutch.tools;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader;

import org.apache.nutch.protocol.Content;
import org.apache.nutch.crawl.CrawlDatum;

import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseText;


public class SquenceFile {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IllegalArgumentException, IOException {
		String usage = "./nutch org.apache.nutch.tools.SquenceFile <name of segment: 	content|crawl_*|parse_data|parse_text> <path of file>";
	    if (args.length < 2) {
	      System.out.println("usage:" + usage);
	      return;
	    }
		System.out.println("Start here...");
		int typeSegment = 0;
		if(args[0].equals("content")){
			typeSegment = 1;
		}else if (args[0].equals("crawl_*")) {
			typeSegment = 2;
		}else if (args[0].equals("parse_data")) {
			typeSegment = 3;
		}else if (args[0].equals("parse_text")) {
			typeSegment = 4;
		}
		switch (typeSegment) {
		case 1:
			read_content(args[1]);
			break;
		case 2:
			read_crawl(args[1]);
			break;
		case 3:
			read_parse_data(args[1]);
			break;
		case 4:
			read_parse_text(args[1]);
			break;
		}
	}
	public static void read_content(String pathFile) throws ClassNotFoundException, SQLException, IllegalArgumentException, IOException {
		Text  key=new Text();
		Content value = new Content();
		Configuration conf = new Configuration();
		SequenceFile.Reader reader=new Reader(FileSystem.get(conf),new Path(pathFile),conf);
		
		//URL_HBase store=new URL_HBase(conf);
		while(reader.next(key,value))
		{
			System.out.println(key.toString());
			System.out.println(value.toString());
		}
		reader.close();
		System.out.println("End");
	}
	public static void read_crawl(String pathFile) throws ClassNotFoundException, SQLException, IllegalArgumentException, IOException {
		Text  key=new Text();
		CrawlDatum value = new CrawlDatum();
		Configuration conf = new Configuration();
		SequenceFile.Reader reader=new Reader(FileSystem.get(conf),new Path(pathFile),conf);
		
		//URL_HBase store=new URL_HBase(conf);
		while(reader.next(key,value))
		{
			System.out.println(key.toString());
			System.out.println(value.toString());
		}
		reader.close();
		System.out.println("End");
	}
	public static void read_parse_data(String pathFile) throws ClassNotFoundException, SQLException, IllegalArgumentException, IOException {
		Text  key=new Text();
		ParseData value = new ParseData();
		Configuration conf = new Configuration();
		SequenceFile.Reader reader=new Reader(FileSystem.get(conf),new Path(pathFile),conf);
		
		//URL_HBase store=new URL_HBase(conf);
		while(reader.next(key,value))
		{
			System.out.println(key.toString());
			System.out.println(value.toString());
		}
		reader.close();
		System.out.println("End");
	}
	public static void read_parse_text(String pathFile) throws ClassNotFoundException, SQLException, IllegalArgumentException, IOException {
		Text  key=new Text();
		ParseText value = new ParseText();
		Configuration conf = new Configuration();
		SequenceFile.Reader reader=new Reader(FileSystem.get(conf),new Path(pathFile),conf);
		
		//URL_HBase store=new URL_HBase(conf);
		while(reader.next(key,value))
		{
			System.out.println(key.toString());
			System.out.println(value.toString());
		}
		reader.close();
		System.out.println("End");
	}
}