package kafka.connect.marklogic.sink;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import kafka.connect.marklogic.MarkLogicBufferedWriter;

import kafka.connect.marklogic.MarkLogicWriter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.common.config.ConfigDef.Recommender;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Sanju Thomas
 *
 */
public class MarkLogicSinkConfig extends AbstractConfig {
    
    private static final Logger logger = LoggerFactory.getLogger(MarkLogicSinkConfig.class);
	
	public static final String CONNECTION_HOST = "ml.connection.host";
	private static final String CONNECTION_HOST_DOC = "ml application server hostname";
	private static final String CONNECTION_HOST_DISPLAY = "Host";
	
	public static final String CONNECTION_PORT = "ml.connection.port";
    private static final String CONNECTION_PORT_DOC = "ml application server port";
    private static final int CONNECTION_PORT_DEFAULT = 8000;
    private static final String CONNECTION_PORT_DISPLAY = "Port";
	
	public static final String CONNECTION_USER = "ml.connection.user";
	private static final String CONNECTION_USER_DOC = "ml connection user.";
	private static final String CONNECTION_USER_DISPLAY = "Username";

	public static final String CONNECTION_PASSWORD = "ml.connection.password";
	private static final String CONNECTION_PASSWORD_DOC = "ml connection password";
	private static final String CONNECTION_PASSWORD_DISPLAY = "Password";

	public static final String BATCH_SIZE = "ml.batch.size";
	private static final int BATCH_SIZE_DEFAULT = 1000;
	private static final String BATCH_SIZE_DOC = "ml batch size";
	
	public static final String WRITER_IMPL = "ml.writer.impl";
	private static final String WRITER_IMPL_DEFAULT = MarkLogicBufferedWriter.class.getCanonicalName();
	private static final String WRITER_IMPL_DOC = "ml writer implementation class name";
	private static final String WRITER_IMPL_DISPLAY = "Marklogic Write Implementation Class";
	
	public static final String MAX_RETRIES = "max.retries";
    private static final int MAX_RETRIES_DEFAULT = 100;
    private static final String MAX_RETRIES_DOC =  "The maximum number of times to retry on errors/exception before failing the task.";
		
	public static final String RETRY_BACKOFF_MS = "retry.backoff.ms";
    private static final int RETRY_BACKOFF_MS_DEFAULT = 10000;
	private static final String RETRY_BACKOFF_MS_DOC = "The time in milliseconds to wait following an error/exception before a retry attempt is made.";

	private static final String MARKLOGIC_CONFIG_GROUP = "Marklogic Connection Details";
	
	private static final Recommender WRITER_IMPL_RECOMMENDER = new Recommender() {
		@Override
		public List<Object> validValues(String s, Map<String, Object> map) {
			return Arrays.<Object>asList(
				MarkLogicBufferedWriter.class,
				MarkLogicWriter.class
			);
		}

		@Override
		public boolean visible(String s, Map<String, Object> map) {
			return true;
		}
    };

	public static ConfigDef CONFIG_DEF = new ConfigDef()
			.define(
				CONNECTION_HOST,
				Type.STRING,
				Importance.HIGH,
				CONNECTION_HOST_DOC,
				MARKLOGIC_CONFIG_GROUP,
				1,
				Width.NONE,
				CONNECTION_HOST_DISPLAY
			)
			.define(
				CONNECTION_PORT,
				Type.INT,
				CONNECTION_PORT_DEFAULT,
				Range.atLeast(0),
				Importance.HIGH,
				CONNECTION_PORT_DOC,
				MARKLOGIC_CONFIG_GROUP,
				2,
				Width.NONE,
				CONNECTION_PORT_DISPLAY
			)
			.define(
				CONNECTION_USER,
				Type.STRING,
				Importance.HIGH,
				CONNECTION_USER_DOC,
				MARKLOGIC_CONFIG_GROUP,
				3,
				Width.NONE,
				CONNECTION_USER_DISPLAY
			)
			.define(
				CONNECTION_PASSWORD,
				Type.PASSWORD,
				Importance.LOW,
				CONNECTION_PASSWORD_DOC,
				MARKLOGIC_CONFIG_GROUP,
				4,
				Width.NONE,
				CONNECTION_PASSWORD_DISPLAY
			)
			.define(BATCH_SIZE, Type.INT, BATCH_SIZE_DEFAULT, Importance.MEDIUM, BATCH_SIZE_DOC)
			.define(MAX_RETRIES, Type.INT, MAX_RETRIES_DEFAULT, Importance.MEDIUM, MAX_RETRIES_DOC)
			.define(RETRY_BACKOFF_MS, Type.INT, RETRY_BACKOFF_MS_DEFAULT, Importance.MEDIUM, RETRY_BACKOFF_MS_DOC)
			.define(
				WRITER_IMPL,
				Type.CLASS,
				WRITER_IMPL_DEFAULT,
				Importance.MEDIUM,
				WRITER_IMPL_DOC,
				"Writer",
				1,
				Width.NONE,
				WRITER_IMPL_DISPLAY,
				WRITER_IMPL_RECOMMENDER
			);

	public MarkLogicSinkConfig(final Map<?, ?> originals) {
		
		super(CONFIG_DEF, originals, false);
		logger.info("Original Configs {}", originals);
	}

}
