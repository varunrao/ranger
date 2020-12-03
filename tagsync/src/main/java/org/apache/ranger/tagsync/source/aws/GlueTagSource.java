/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.ranger.tagsync.source.aws;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClient;
import com.amazonaws.regions.Regions;

import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.GetTagsRequest;
import com.amazonaws.services.glue.model.GetTagsResult;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.util.ServiceTags;
import org.apache.ranger.tagsync.model.AbstractTagSource;
import org.apache.ranger.tagsync.model.TagSink;
import org.apache.ranger.tagsync.process.TagSyncConfig;
import org.apache.ranger.tagsync.process.TagSynchronizer;
import org.json.JSONObject;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

public class GlueTagSource extends AbstractTagSource implements Runnable {
	private static final Log LOG = LogFactory.getLog(GlueTagSource.class);

//	private String serviceTagsFileName;
	private URL serviceTagsFileURL;
	private long lastModifiedTimeInMillis = -1L;

	private Gson gsonBuilder;
	private long fileModTimeCheckIntervalInMs;

	private Thread myThread = null;

	private String serviceTagsFileName = "{\n" +
			"     \"op\": \"add_or_update\",\n" +
			"     \"serviceName\": \"AUTH_HIVE\",\n" +
			"     \"tagVersion\": 3,\n" +
			"     \"tagDefinitions\": {\n" +
			"     \"1\":{\"id\":1, \"guid\":\"tagdef-1\", \"name\":\"PII\", \"attributeDefs\":[], \"owner\":0}\n" +
			"     },\n" +
			"     \"tags\": {\n" +
			"      \"1\": {\n" +
			"         \"type\": \"PII\",\n" +
			"         \"attributes\": {},\n" +
			"         \"id\": 2,\n" +
			"         \"guid\": \"tag-pii-2-guid\"\n" +
			"       }\n" +
			"     },\n" +
			"     \"serviceResources\": [\n" +
			"       {\n" +
			"         \"serviceName\": \"AUTH_HIVE\",\n" +
			"         \"resourceElements\": {\n" +
			"           \"database\": { \"values\": [ \"retail\" ] },\n" +
			"           \"table\": { \"values\": [ \"products\" ] }\n" +
			"         },\n" +
			"         \"id\": 1,\n" +
			"         \"guid\": \"retail.products\"\n" +
			"       }\n" +
			"     ],\n" +
			"     \"resourceToTagIds\": {\n" +
			"       \"1\": [ 1 ],\n" +
			"     }\n" +
			"   }";
	public static void main(String[] args) {

		GlueTagSource fileTagSource = new GlueTagSource();

		TagSyncConfig config = TagSyncConfig.getInstance();

		Properties props = config.getProperties();

		if (args.length > 0) {
			String tagSourceFileName = args[0];
			LOG.info("TagSourceFileName is set to " + args[0]);
			props.setProperty(TagSyncConfig.TAGSYNC_FILESOURCE_FILENAME_PROP, tagSourceFileName);
		}

		TagSynchronizer.printConfigurationProperties(props);

		boolean ret = TagSynchronizer.initializeKerberosIdentity(props);

		if (ret) {
			TagSink tagSink = TagSynchronizer.initializeTagSink(props);

			if (tagSink != null) {

				if (fileTagSource.initialize(props)) {
					try {
						tagSink.start();
						fileTagSource.setTagSink(tagSink);
						fileTagSource.synchUp();
					} catch (Exception exception) {
						LOG.error("ServiceTags upload failed : ", exception);
						System.exit(1);
					}
				} else {
					LOG.error("FileTagSource initialized failed, exiting.");
					System.exit(1);
				}

			} else {
				LOG.error("TagSink initialialization failed, exiting.");
				System.exit(1);
			}
		} else {
			LOG.error("Error initializing kerberos identity");
			System.exit(1);
		}

	}

	@Override
	public boolean initialize(Properties props) {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> FileTagSource.initialize()");
		}

		Properties properties;

		if (props == null || MapUtils.isEmpty(props)) {
			LOG.error("No properties specified for FileTagSource initialization");
			properties = new Properties();
		} else {
			properties = props;
		}

		gsonBuilder = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z").setPrettyPrinting().create();

		boolean ret = true;

		serviceTagsFileName = TagSyncConfig.getTagSourceFileName(properties);

		if (StringUtils.isBlank(serviceTagsFileName)) {
			ret = false;
			LOG.error("value of property 'ranger.tagsync.source.impl.class' is file and no value specified for property 'ranger.tagsync.filesource.filename'!");
		}

		if (ret) {

			fileModTimeCheckIntervalInMs = TagSyncConfig.getTagSourceFileModTimeCheckIntervalInMillis(properties);

			if (LOG.isDebugEnabled()) {
				LOG.debug("Provided serviceTagsFileName=" + serviceTagsFileName);
				LOG.debug("'ranger.tagsync.filesource.modtime.check.interval':" + fileModTimeCheckIntervalInMs + "ms");
			}

			InputStream serviceTagsFileStream = null;

			File f = new File(serviceTagsFileName);

			if (f.exists() && f.isFile() && f.canRead()) {
				try {
					serviceTagsFileStream = new FileInputStream(f);
					serviceTagsFileURL = f.toURI().toURL();
				} catch (FileNotFoundException exception) {
					LOG.error("Error processing input file:" + serviceTagsFileName + " or no privilege for reading file " + serviceTagsFileName, exception);
				} catch (MalformedURLException malformedException) {
					LOG.error("Error processing input file:" + serviceTagsFileName + " cannot be converted to URL " + serviceTagsFileName, malformedException);
				}
			} else {

				URL fileURL = getClass().getResource(serviceTagsFileName);
				if (fileURL == null) {
					if (!serviceTagsFileName.startsWith("/")) {
						fileURL = getClass().getResource("/" + serviceTagsFileName);
					}
				}

				if (fileURL == null) {
					fileURL = ClassLoader.getSystemClassLoader().getResource(serviceTagsFileName);
					if (fileURL == null) {
						if (!serviceTagsFileName.startsWith("/")) {
							fileURL = ClassLoader.getSystemClassLoader().getResource("/" + serviceTagsFileName);
						}
					}
				}

				if (fileURL != null) {

					try {
						serviceTagsFileStream = fileURL.openStream();
						serviceTagsFileURL = fileURL;
					} catch (Exception exception) {
						LOG.error(serviceTagsFileName + " is not a file", exception);
					}
				} else {
					LOG.warn("Error processing input file: URL not found for " + serviceTagsFileName + " or no privilege for reading file " + serviceTagsFileName);
				}
			}

			if (serviceTagsFileStream != null) {
				try {
					serviceTagsFileStream.close();
				} catch (Exception e) {
					// Ignore
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== FileTagSource.initialize(): sourceFileName=" + serviceTagsFileName + ", result=" + ret);
		}

		return ret;
	}

	@Override
	public boolean start() {

		myThread = new Thread(this);
		myThread.setDaemon(true);
		myThread.start();

		return true;
	}

	@Override
	public void stop() {
		if (myThread != null && myThread.isAlive()) {
			myThread.interrupt();
		}
	}

	@Override
	public void run() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> FileTagSource.run()");
		}

		while (true) {

			try {
				synchUp();

				LOG.debug("Sleeping for [" + fileModTimeCheckIntervalInMs + "] milliSeconds");

				Thread.sleep(fileModTimeCheckIntervalInMs);
			}
			catch (InterruptedException exception) {
				LOG.error("Interrupted..: ", exception);
				return;
			}
		}
	}

	private boolean isChanged() {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> FileTagSource.isChanged()");
		}
		boolean ret = false;

		long modificationTime = getModificationTime();

		if (modificationTime > lastModifiedTimeInMillis) {
			if (LOG.isDebugEnabled()) {
				Date modifiedDate = new Date(modificationTime);
				Date lastModifiedDate = new Date(lastModifiedTimeInMillis);
				LOG.debug("File modified at " + modifiedDate + "last-modified at " + lastModifiedDate);
			}
			lastModifiedTimeInMillis = modificationTime;
			ret = true;
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== FileTagSource.isChanged(): result=" + ret);
		}
		return ret;
	}

	public void synchUp() {
		if (isChanged()) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Begin: update tags from source==>sink");
			}

			ServiceTags serviceTags = readFromFile();
			updateSink(serviceTags);

			if (LOG.isDebugEnabled()) {
				LOG.debug("End: update tags from source==>sink");
			}

		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug("FileTagSource: no change found for synchronization.");
			}
		}
	}
	private ServiceTags readFromFile() {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> FileTagSource.readFromFile(): sourceFileName=" + serviceTagsFileName);
		}

		ServiceTags ret = null;

		if (serviceTagsFileURL != null) {
			try (
					InputStream serviceTagsFileStream = serviceTagsFileURL.openStream();
					Reader reader = new InputStreamReader(serviceTagsFileStream, Charset.forName("UTF-8"))
			) {

				ret = gsonBuilder.fromJson(reader, ServiceTags.class);

			} catch (IOException e) {
				LOG.warn("Error processing input file: or no privilege for reading file " + serviceTagsFileName, e);
			}
		} else {
			LOG.error("Error reading file: " + serviceTagsFileName);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== FileTagSource.readFromFile(): sourceFileName=" + serviceTagsFileName);
		}

		return ret;
	}


	private ServiceTags readFromGlueCatalog() {

		AWSGlue glueCleint = AWSGlueClient.builder().withRegion(Regions.US_EAST_1).build();
		GetTagsRequest request =
				new GetTagsRequest();
		request.setResourceArn("arn:aws:glue:us-east-1:123456789012:retail/products");
		GetTagsResult result = glueCleint.getTags(request);
		JSONObject tomJsonObject = new JSONObject(serviceTagsFileName);
		JSONObject obj = tomJsonObject.getJSONObject("tags").getJSONObject("1");
		obj.put("type", result.getTags().get("securityTag"));

		System.out.println(tomJsonObject);
		//tomJsonObject.

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> FileTagSource.readFromFile(): sourceTag=" + tomJsonObject);
		}

		ServiceTags ret = null;

		if (tomJsonObject != null) {
			try {

				ret = gsonBuilder.fromJson(tomJsonObject.toString(), ServiceTags.class);

			} catch (Exception e) {
				LOG.warn("Error processing input file: or no privilege for reading file " + serviceTagsFileName, e);
			}
		} else {
			LOG.error("Error reading file: " + tomJsonObject);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== FileTagSource.readFromFile(): sourceFileName=" + serviceTagsFileName);
		}

		return ret;
	}

	private long getModificationTime() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> FileTagSource.getLastModificationTime(): sourceFileName=" + serviceTagsFileName);
		}
		long ret = 0L;

		File sourceFile = new File(serviceTagsFileName);

		if (sourceFile.exists() && sourceFile.isFile() && sourceFile.canRead()) {
			ret = sourceFile.lastModified();
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== FileTagSource.lastModificationTime(): serviceTagsFileName=" + serviceTagsFileName + " result=" + new Date(ret));
		}

		return ret;
	}
}
