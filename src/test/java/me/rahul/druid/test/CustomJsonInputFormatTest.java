package me.rahul.druid.test;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

import me.rahul.druid.CustomExtensionModule;
import me.rahul.druid.CustomJsonInputFormat;

import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.junit.Assert;
import org.junit.Test;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CustomJsonInputFormatTest {
	private static final ObjectMapper MAPPER = new DefaultObjectMapper();
	
	static {
		for (Module module : new CustomExtensionModule().getJacksonModules()) {
			MAPPER.registerModule(module);
		}
	}

	@Test
	public void testCustomJsonInputFormat_WithoutNewlineJson() throws IOException {
		String jsonStr = "{\"_id\":\"Test2-1703072029159\",\"deviceID\":\"Test2\",\"content\":{\"timestamp\":1703072029159,\"temperature\":30.1}}";
		ByteEntity source = new ByteEntity(StringUtils.toUtf8(jsonStr));
		String flattenSpecJson = "{\"useFieldDiscovery\":true,\"fields\":[{\"type\":\"path\",\"name\":\"content.timestamp\",\"expr\":\"$.content.timestamp\",\"nodes\":null},{\"type\":\"path\",\"name\":\"content.temperature\",\"expr\":\"$.content.temperature\",\"nodes\":null}]}";
		List<String> dimensions = ImmutableList.of("_id", "deviceID", "content.timestamp", "content.temperature", "ingestionTime");
		
		JSONPathSpec flattenSpec = MAPPER.readValue(flattenSpecJson, JSONPathSpec.class);
		Map<String, Boolean> featureSpec = new HashMap<>();
		boolean keepNullColumns = true;
		boolean assumeNewlineDelimited = false;
		boolean addIngestionTime = true;
		
		CustomJsonInputFormat customJsonInputFormat = new CustomJsonInputFormat(flattenSpec, featureSpec, keepNullColumns, assumeNewlineDelimited, addIngestionTime);
		InputEntityReader reader = customJsonInputFormat.createReader(new InputRowSchema(new TimestampSpec("content.timestamp", "millis", null),
				new DimensionsSpec(DimensionsSpec.getDefaultSchemas(dimensions)),
				ColumnsFilter.all()), source, null);
		
		try (CloseableIterator<InputRow> iterator = reader.read()) {
			InputRow row = iterator.next();
		    Assert.assertNotNull(row);
	        Assert.assertEquals("Test2-1703072029159", row.getRaw("_id"));
	        Assert.assertEquals("Test2", row.getRaw("deviceID"));
	        Assert.assertEquals(1703072029159L, row.getRaw("content.timestamp"));
	        Assert.assertEquals(30.1, row.getRaw("content.temperature"));		
	        Assert.assertTrue(row.getRaw("ingestionTime") != null);
		}
	}
	
	@Test
	public void testCustomJsonInputFormat_WithNewlineJson() throws IOException {
		String jsonStr = "{\"_id\":\"Test1-1703072029159\",\"deviceID\":\"Test1\",\"content\":{\"timestamp\":1703072029159,\"temperature\":45.1}}\n"
				+ "{\"_id\":\"Test2-1703072029159\",\"deviceID\":\"Test2\",\"content\":{\"timestamp\":1703072029159,\"temperature\":30.1}}";
		ByteEntity source = new ByteEntity(StringUtils.toUtf8(jsonStr));
		String flattenSpecJson = "{\"useFieldDiscovery\":true,\"fields\":[{\"type\":\"path\",\"name\":\"content.timestamp\",\"expr\":\"$.content.timestamp\",\"nodes\":null},{\"type\":\"path\",\"name\":\"content.temperature\",\"expr\":\"$.content.temperature\",\"nodes\":null}]}";
		List<String> dimensions = ImmutableList.of("_id", "deviceID", "content.timestamp", "content.temperature");
		
		JSONPathSpec flattenSpec = MAPPER.readValue(flattenSpecJson, JSONPathSpec.class);
		Map<String, Boolean> featureSpec = new HashMap<>();
		boolean keepNullColumns = true;
		boolean assumeNewlineDelimited = true;
		boolean addIngestionTime = false;
		
		CustomJsonInputFormat customJsonInputFormat = new CustomJsonInputFormat(flattenSpec, featureSpec, keepNullColumns, assumeNewlineDelimited, addIngestionTime);
		InputEntityReader reader = customJsonInputFormat.createReader(new InputRowSchema(new TimestampSpec("content.timestamp", "millis", null),
				new DimensionsSpec(DimensionsSpec.getDefaultSchemas(dimensions)),
				ColumnsFilter.all()), source, null);
		
		try (CloseableIterator<InputRow> iterator = reader.read()) {
			InputRow row1 = iterator.next();
			InputRow row2 = iterator.next();
						
			 // Validate fields for row1
			Assert.assertNotNull(row1);
			Assert.assertEquals("Test1-1703072029159", row1.getRaw("_id"));
			Assert.assertEquals("Test1", row1.getRaw("deviceID"));
			Assert.assertEquals(1703072029159L, row1.getRaw("content.timestamp"));
			Assert.assertEquals(45.1, row1.getRaw("content.temperature"));
			Assert.assertTrue(row1.getRaw("ingestionTime") == null);

	        // Validate fields for row2
	        Assert.assertNotNull(row2);
	        Assert.assertEquals("Test2-1703072029159", row2.getRaw("_id"));
	        Assert.assertEquals("Test2", row2.getRaw("deviceID"));
	        Assert.assertEquals(1703072029159L, row2.getRaw("content.timestamp"));
	        Assert.assertEquals(30.1, row2.getRaw("content.temperature"));		
	        Assert.assertTrue(row2.getRaw("ingestionTime") == null);
		}
	}
}
