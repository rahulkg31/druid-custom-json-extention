package me.rahul.druid;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterators;
import org.apache.commons.io.IOUtils;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.IntermediateRowParsingReader;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.JSONFlattenerMaker;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.common.parsers.ObjectFlattener;
import org.apache.druid.java.util.common.parsers.ObjectFlatteners;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.utils.CollectionUtils;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * In contrast to {@link JsonLineReader} which processes input text line by line
 * independently, this class tries to parse the input text as a whole to an
 * array of objects.
 *
 * The input text can be: 1. a JSON string of an object in a line or multiple
 * lines(such as pretty-printed JSON text) 2. multiple JSON object strings
 * concated by white space character(s)
 *
 * For case 2, what should be noticed is that if an exception is thrown when
 * parsing one JSON string, the rest JSON text will all be ignored
 *
 * For more information, see: https://github.com/apache/druid/pull/10383
 */
public class JsonReader extends IntermediateRowParsingReader<String> {
	private final ObjectFlattener<JsonNode> flattener;
	private final ObjectMapper mapper;
	private final InputEntity source;
	private final InputRowSchema inputRowSchema;
	private final JsonFactory jsonFactory;
	private final boolean addIngestionTime;

	JsonReader(InputRowSchema inputRowSchema, InputEntity source, JSONPathSpec flattenSpec, ObjectMapper mapper,
			boolean keepNullColumns, boolean addIngestionTime) {
		this.inputRowSchema = inputRowSchema;
		this.source = source;
		this.flattener = ObjectFlatteners.create(flattenSpec, new JSONFlattenerMaker(keepNullColumns));
		this.mapper = mapper;
		this.jsonFactory = new JsonFactory();
		this.addIngestionTime = addIngestionTime;
	}

	@Override
	protected CloseableIterator<String> intermediateRowIterator() throws IOException {
		return CloseableIterators.withEmptyBaggage(
				Iterators.singletonIterator(IOUtils.toString(source.open(), StringUtils.UTF8_STRING)));
	}

	@Override
	protected InputEntity source() {
		return source;
	}

	@Override
	protected List<InputRow> parseInputRows(String intermediateRow) throws IOException, ParseException {
		final List<InputRow> inputRows;
		try (JsonParser parser = jsonFactory.createParser(intermediateRow)) {
			final MappingIterator<JsonNode> delegate = mapper.readValues(parser, JsonNode.class);
			inputRows = FluentIterable.from(() -> delegate)
					.transform(jsonNode -> MapInputRowParser.parse(inputRowSchema, flatten(jsonNode))).toList();
		} catch (RuntimeException e) {
			// convert Jackson's JsonParseException into druid's exception for further
			// processing
			// JsonParseException will be thrown from MappingIterator#hasNext or
			// MappingIterator#next when input json text is ill-formed
			if (e.getCause() instanceof JsonParseException) {
				throw new ParseException(intermediateRow, e, "Unable to parse row [%s]", intermediateRow);
			}

			// throw unknown exception
			throw e;
		}
		if (CollectionUtils.isNullOrEmpty(inputRows)) {
			throw new ParseException(intermediateRow,
					"Unable to parse [%s] as the intermediateRow resulted in empty input row", intermediateRow);
		}
		return inputRows;
	}

	@Override
	protected List<Map<String, Object>> toMap(String intermediateRow) throws IOException {
		try (JsonParser parser = jsonFactory.createParser(intermediateRow)) {
			final MappingIterator<Map> delegate = mapper.readValues(parser, Map.class);
			return FluentIterable.from(() -> delegate).transform(map -> (Map<String, Object>) map).toList();
		} catch (RuntimeException e) {
			// convert Jackson's JsonParseException into druid's exception for further
			// processing
			// JsonParseException will be thrown from MappingIterator#hasNext or
			// MappingIterator#next when input json text is ill-formed
			if (e.getCause() instanceof JsonParseException) {
				throw new ParseException(intermediateRow, e, "Unable to parse row [%s]", intermediateRow);
			}

			// throw unknown exception
			throw e;
		}
	}

	private Map<String, Object> flatten(JsonNode node) {
		ObjectNode document = (ObjectNode) node;
		if (addIngestionTime) {
			document.put("ingestionTime", Instant.now().getMillis());
		}		
		return flattener.flatten(document);
	}
}