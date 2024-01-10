package me.rahul.druid;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.TextReader;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.java.util.common.parsers.JSONFlattenerMaker;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.common.parsers.ObjectFlattener;
import org.apache.druid.java.util.common.parsers.ObjectFlatteners;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * {@link JsonLineReader} reads input text line by line and tries to convert
 * each text line to an JSON object.
 *
 * Since each text line is processed indepdently, if any exception is thrown
 * when parsing one text line, exception can be caught by callers to skip
 * current line and continue to process next text line.
 *
 * This also means that each text line should be a well-formed JSON text,
 * pretty-printed format is not allowed
 *
 */
public class JsonLineReader extends TextReader {
	private final ObjectFlattener<JsonNode> flattener;
	private final ObjectMapper mapper;
	private final boolean addIngestionTime;

	JsonLineReader(InputRowSchema inputRowSchema, InputEntity source, JSONPathSpec flattenSpec, ObjectMapper mapper,
			boolean keepNullColumns, boolean addIngestionTime) {
		super(inputRowSchema, source);
		this.flattener = ObjectFlatteners.create(flattenSpec, new JSONFlattenerMaker(keepNullColumns));
		this.mapper = mapper;
		this.addIngestionTime = addIngestionTime;
	}

	@Override
	public List<InputRow> parseInputRows(String line) throws IOException, ParseException {
		final ObjectNode document = (ObjectNode) mapper.readValue(line, JsonNode.class);
		if(addIngestionTime) {
			document.put("ingestionTime", Instant.now().getMillis());
		}		
		final Map<String, Object> flattened = flattener.flatten(document);
		return Collections.singletonList(MapInputRowParser.parse(getInputRowSchema(), flattened));
	}

	@Override
	public List<Map<String, Object>> toMap(String intermediateRow) throws IOException {
		// noinspection unchecked
		return Collections.singletonList(mapper.readValue(intermediateRow, Map.class));
	}

	@Override
	public int getNumHeaderLinesToSkip() {
		return 0;
	}

	@Override
	public boolean needsToProcessHeaderLine() {
		return false;
	}

	@Override
	public void processHeaderLine(String line) {
		// do nothing
	}

}