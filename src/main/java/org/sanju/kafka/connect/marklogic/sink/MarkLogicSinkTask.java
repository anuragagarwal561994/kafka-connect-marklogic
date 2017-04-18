package org.sanju.kafka.connect.marklogic.sink;

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.sanju.kafka.connect.marklogic.BufferedMarkLogicWriter;
import org.sanju.kafka.connect.marklogic.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Sanju Thomas
 *
 */
public class MarkLogicSinkTask extends SinkTask {

    private static final Logger logger = LoggerFactory.getLogger(MarkLogicSinkTask.class);
    private int timeout;
    private Writer writer;
    private Map<String, String> config;

    @Override
    public void put(final Collection<SinkRecord> records) {

        if (records.isEmpty()) {
            logger.debug("Empty record collection to process");
            return;
        }

        final SinkRecord first = records.iterator().next();
        final int recordsCount = records.size();
        logger.info("Received {} records. kafka coordinates from record: Topic - {}, Partition - {}, Offset - {}",
                        recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset());

        try {
            writer.write(records);
        } catch (final RetriableException e) {
            logger.warn("Setting the task timeout to {} ms upon RetriableException", timeout);
            this.writer = new BufferedMarkLogicWriter(config);
            context.timeout(timeout);
            throw e;
        }
    }

    @Override
    public void start(final Map<String, String> config) {

        logger.info("start called!");
        this.config = config;
        this.timeout = Integer.valueOf(config.get(MarkLogicSinkConfig.RETRY_BACKOFF_MS));
        this.writer = new BufferedMarkLogicWriter(config);
    }

    @Override
    public void stop() {

        logger.info("stop called!");
    }

    @Override
    public void flush(final Map<TopicPartition, OffsetAndMetadata> currentOffsets) {

        currentOffsets.forEach((k, v) -> logger.info("Flush - Topic {}, Partition {}, Offset {}, Metadata {}",
                k.topic(), k.partition(), v.offset(), v.metadata()));
    }

    public String version() {

        return MarkLogicSinkConnector.MARKLOGIC_CONNECTOR_VERSION;
    }

}
