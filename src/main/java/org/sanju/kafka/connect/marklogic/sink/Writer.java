package org.sanju.kafka.connect.marklogic.sink;

import java.util.List;

import org.apache.kafka.connect.sink.SinkRecord;

/**
 * 
 * @author Sanju Thomas
 *
 */
public interface Writer {
	
	void write(List<SinkRecord> records);
}