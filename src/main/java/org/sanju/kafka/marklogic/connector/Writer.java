package org.sanju.kafka.marklogic.connector;

import java.util.List;

import org.apache.kafka.connect.sink.SinkRecord;

/**
 * 
 * @author Sanju Thomas
 *
 */
public interface Writer {
	
	boolean write(List<SinkRecord> records);
}
