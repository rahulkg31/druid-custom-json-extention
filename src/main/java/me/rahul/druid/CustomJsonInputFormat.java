package me.rahul.druid;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.databind.ObjectMapper;


import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.NestedInputFormat;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

public class CustomJsonInputFormat extends NestedInputFormat {
	public static final String TYPE_KEY = "custom-json";
	private final Map<String, Boolean> featureSpec;
	private final ObjectMapper objectMapper;
	private final boolean keepNullColumns;
	private final boolean assumeNewlineDelimited;
	private final boolean addIngestionTime;

	@JsonCreator
	public CustomJsonInputFormat(@JsonProperty("flattenSpec") @Nullable JSONPathSpec flattenSpec,
			@JsonProperty("featureSpec") @Nullable Map<String, Boolean> featureSpec,
			@JsonProperty("keepNullColumns") @Nullable Boolean keepNullColumns,
			@JsonProperty("assumeNewlineDelimited") @Nullable Boolean assumeNewlineDelimited,
			@JsonProperty("addIngestionTime") @Nullable Boolean addIngestionTime) {
		super(flattenSpec);
		this.featureSpec = featureSpec == null ? Collections.emptyMap() : featureSpec;
		this.objectMapper = new ObjectMapper();
		if (keepNullColumns != null) {
			this.keepNullColumns = keepNullColumns;
		} else {
			this.keepNullColumns = flattenSpec != null && flattenSpec.isUseFieldDiscovery();
		}
		for (Entry<String, Boolean> entry : this.featureSpec.entrySet()) {
			Feature feature = Feature.valueOf(entry.getKey());
			objectMapper.configure(feature, entry.getValue());
		}
		this.assumeNewlineDelimited = assumeNewlineDelimited != null && assumeNewlineDelimited;
		this.addIngestionTime = addIngestionTime != null && addIngestionTime;
	}

	@JsonProperty
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	public Map<String, Boolean> getFeatureSpec() {
		return featureSpec;
	}

	@JsonProperty 
	public boolean isKeepNullColumns() {
		return keepNullColumns;
	}

	@JsonProperty
	public boolean isAssumeNewlineDelimited() {
		return assumeNewlineDelimited;
	}
	
	@JsonProperty
	public boolean isAddIngestionTime() {
		return addIngestionTime;
	}

	@Override
	public InputEntityReader createReader(InputRowSchema inputRowSchema, InputEntity source, File temporaryDirectory) {
		if (assumeNewlineDelimited) {
			return new JsonLineReader(inputRowSchema, source, getFlattenSpec(), objectMapper, keepNullColumns, addIngestionTime);
		} else {
			return new JsonReader(inputRowSchema, source, getFlattenSpec(), objectMapper, keepNullColumns, addIngestionTime);
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}
		CustomJsonInputFormat that = (CustomJsonInputFormat) o;
		return keepNullColumns == that.keepNullColumns && assumeNewlineDelimited == that.assumeNewlineDelimited && Objects.equals(featureSpec, that.featureSpec);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), featureSpec, keepNullColumns, assumeNewlineDelimited);
	}

	@Override
	public boolean isSplittable() {
		return false;
	}
}