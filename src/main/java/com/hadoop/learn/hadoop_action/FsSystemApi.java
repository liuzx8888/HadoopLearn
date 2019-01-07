package com.hadoop.learn.hadoop_action;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class FsSystemApi {
	private static Configuration conf = null;
	private static FSDataInputStream in = null;
	private static FileSystem fs = null;

	private Configuration getconf() {
		conf = new Configuration();
		return conf;
	}

	private FileSystem getfs(URI uri, Configuration conf) {

		try {
			fs = FileSystem.get(uri, conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return fs;
	}

	private void FsCat(String uri) {
		try {
			in = fs.open(new Path(uri));
			IOUtils.copyBytes(in, System.out, 4096, false);
			/* 带有偏移量的查看数据 */
			in.seek(50);
			IOUtils.copyBytes(in, System.out, 4096, false);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			// TODO: handle finally clause
			IOUtils.closeStream(in);
		}
	}

	private String mkdir(FileSystem fs, String path) {
		try {
			if (fs.exists(new Path(path))) {
				System.out.println("该路径文件夹已存在！！");
			} else {
				fs.mkdirs(new Path(path));
				System.out.println("该路径文件夹已创建成功！！");
			}

		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;

	}

	public static void main(String[] args) throws Exception {

		String uri = args[0];
		FsSystemApi fsSystemApi = new FsSystemApi();

		conf = fsSystemApi.getconf();
		fs = fsSystemApi.getfs(new URI(uri), conf);
		fsSystemApi.FsCat(uri);

	}

}
