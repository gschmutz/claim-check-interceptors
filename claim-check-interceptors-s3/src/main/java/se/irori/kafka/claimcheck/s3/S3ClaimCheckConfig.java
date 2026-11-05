package se.irori.kafka.claimcheck.s3;

import static se.irori.kafka.claimcheck.s3.S3ClaimCheckConfig.Keys.S3_ACCESS_KEY_CONFIG;
import static se.irori.kafka.claimcheck.s3.S3ClaimCheckConfig.Keys.S3_BUCKET_NAME_CONFIG;
import static se.irori.kafka.claimcheck.s3.S3ClaimCheckConfig.Keys.S3_CREATE_BUCKET_IF_NOT_EXISTS;
import static se.irori.kafka.claimcheck.s3.S3ClaimCheckConfig.Keys.S3_OBJECT_PREFIX_CONFIG;
import static se.irori.kafka.claimcheck.s3.S3ClaimCheckConfig.Keys.S3_PARTITION_GRANULARITY_CONFIG;
import static se.irori.kafka.claimcheck.s3.S3ClaimCheckConfig.Keys.S3_PATH_STYLE_ACCESS_CONFIG;
import static se.irori.kafka.claimcheck.s3.S3ClaimCheckConfig.Keys.S3_REGION_CONFIG;
import static se.irori.kafka.claimcheck.s3.S3ClaimCheckConfig.Keys.S3_SECRET_KEY_CONFIG;
import static se.irori.kafka.claimcheck.s3.S3ClaimCheckConfig.Keys.S3_SERVICE_ENDPOINT_CONFIG;
import static se.irori.kafka.claimcheck.s3.S3ClaimCheckConfig.Keys.S3_TOPIC_TRANSFORM_REGEX_CONFIG;
import static se.irori.kafka.claimcheck.s3.S3ClaimCheckConfig.Keys.S3_TOPIC_TRANSFORM_REPLACEMENT_CONFIG;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

/**
 * Configuration for S3 (AWS S3 or S3 compatible, e.g. MinIO) claim check interceptors.
 */
public class S3ClaimCheckConfig extends AbstractConfig {

  /**
   * Placeholder usable in bucket name and object prefix, replaced with the (transformed) topic.
   */
  public static final String TOPIC_PLACEHOLDER = "${topic}";

  public static final String BUCKET_NAME_DOCS = "Target bucket for the stored objects. May contain"
      + " the placeholder `" + TOPIC_PLACEHOLDER + "`, which is replaced by the topic name.";

  public static final String OBJECT_PREFIX_DOCS = "Optional object key prefix (\"folder\") under"
      + " which objects are stored. May contain the placeholder `" + TOPIC_PLACEHOLDER + "`.";

  public static final String PARTITION_GRANULARITY_DOCS = "Time based partitioning of the object"
      + " keys (UTC). One of `none`, `monthly`, `daily`, `hourly`.";

  public static final String TOPIC_TRANSFORM_REGEX_DOCS = "Optional regex applied to the topic name"
      + " before it is used for the `" + TOPIC_PLACEHOLDER + "` placeholder.";

  public static final String TOPIC_TRANSFORM_REPLACEMENT_DOCS = "Replacement string used with `"
      + S3_TOPIC_TRANSFORM_REGEX_CONFIG + "`.";

  public static final String SERVICE_ENDPOINT_DOCS = "S3 service endpoint, e.g. for MinIO."
      + " If not set, the standard AWS endpoint for the configured region is used.";

  public static final String REGION_DOCS = "S3 signing region.";

  public static final String ACCESS_KEY_DOCS = "S3 access key. If not set (together with `"
      + S3_SECRET_KEY_CONFIG + "`), the AWS default credentials provider chain is used.";

  public static final String SECRET_KEY_DOCS = "S3 secret key. Must be set together with `"
      + S3_ACCESS_KEY_CONFIG + "`.";

  public static final String PATH_STYLE_ACCESS_DOCS = "Use path style bucket access."
      + " Required by most S3 compatible stores such as MinIO.";

  public static final String CREATE_BUCKET_DOCS = "Create the bucket if it does not exist."
      + " Requires bucket creation permissions.";

  /** Supported time based partition granularities. */
  enum PartitionGranularity {
    NONE, MONTHLY, DAILY, HOURLY
  }

  static ConfigDef buildConfigDef(ConfigDef base) {
    base.define(S3_BUCKET_NAME_CONFIG, ConfigDef.Type.STRING, "large-object-bucket",
        ConfigDef.Importance.HIGH, BUCKET_NAME_DOCS);

    base.define(S3_OBJECT_PREFIX_CONFIG, ConfigDef.Type.STRING, "",
        ConfigDef.Importance.MEDIUM, OBJECT_PREFIX_DOCS);

    base.define(S3_PARTITION_GRANULARITY_CONFIG, ConfigDef.Type.STRING, "none",
        ConfigDef.Importance.LOW, PARTITION_GRANULARITY_DOCS);

    base.define(S3_TOPIC_TRANSFORM_REGEX_CONFIG, ConfigDef.Type.STRING, "",
        ConfigDef.Importance.LOW, TOPIC_TRANSFORM_REGEX_DOCS);

    base.define(S3_TOPIC_TRANSFORM_REPLACEMENT_CONFIG, ConfigDef.Type.STRING, "",
        ConfigDef.Importance.LOW, TOPIC_TRANSFORM_REPLACEMENT_DOCS);

    base.define(S3_SERVICE_ENDPOINT_CONFIG, ConfigDef.Type.STRING, null,
        ConfigDef.Importance.MEDIUM, SERVICE_ENDPOINT_DOCS);

    base.define(S3_REGION_CONFIG, ConfigDef.Type.STRING, "us-east-1",
        ConfigDef.Importance.MEDIUM, REGION_DOCS);

    base.define(S3_ACCESS_KEY_CONFIG, ConfigDef.Type.STRING, null,
        ConfigDef.Importance.MEDIUM, ACCESS_KEY_DOCS);

    base.define(S3_SECRET_KEY_CONFIG, ConfigDef.Type.PASSWORD, null,
        ConfigDef.Importance.MEDIUM, SECRET_KEY_DOCS);

    base.define(S3_PATH_STYLE_ACCESS_CONFIG, ConfigDef.Type.BOOLEAN, true,
        ConfigDef.Importance.LOW, PATH_STYLE_ACCESS_DOCS);

    base.define(S3_CREATE_BUCKET_IF_NOT_EXISTS, ConfigDef.Type.BOOLEAN, false,
        ConfigDef.Importance.MEDIUM, CREATE_BUCKET_DOCS);

    return base;
  }

  /**
   * Construct, parse and validate a map of configurations.
   *
   * @param originals config options
   * @return a validated configuration object
   * @throws ConfigException if any config option is invalid
   */
  public static S3ClaimCheckConfig validatedConfig(Map<?, ?> originals) {
    S3ClaimCheckConfig config = new S3ClaimCheckConfig(originals);
    config.validate();
    return config;
  }

  private S3ClaimCheckConfig(Map<?, ?> originals) {
    super(buildConfigDef(new ConfigDef()), originals, false);
  }

  /**
   * Get the configured partition granularity.
   *
   * @return the partition granularity
   */
  PartitionGranularity getPartitionGranularity() {
    return PartitionGranularity.valueOf(
        getString(S3_PARTITION_GRANULARITY_CONFIG).trim().toUpperCase(Locale.ROOT));
  }

  /**
   * Validate config properties not covered by {@link ConfigDef#parse(Map)}.
   *
   * @throws ConfigException if configuration is not valid
   */
  private void validate() {
    boolean hasAccessKey = getString(S3_ACCESS_KEY_CONFIG) != null;
    boolean hasSecretKey = getPassword(S3_SECRET_KEY_CONFIG) != null;
    if (hasAccessKey != hasSecretKey) {
      throw new ConfigException("Either set both '" + S3_ACCESS_KEY_CONFIG + "' and '"
          + S3_SECRET_KEY_CONFIG + "', or none of them");
    }

    try {
      getPartitionGranularity();
    } catch (IllegalArgumentException e) {
      throw new ConfigException("Invalid " + S3_PARTITION_GRANULARITY_CONFIG + ": '"
          + getString(S3_PARTITION_GRANULARITY_CONFIG) + "', expected one of "
          + Arrays.toString(PartitionGranularity.values()).toLowerCase(Locale.ROOT));
    }
  }

  /**
   * Config keys for configuring the S3 Claim Check backend.
   */
  public static class Keys {

    public static final String S3_BUCKET_NAME_CONFIG = "s3.bucket.name";

    public static final String S3_OBJECT_PREFIX_CONFIG = "s3.object.prefix";

    public static final String S3_PARTITION_GRANULARITY_CONFIG = "s3.partition.granularity";

    public static final String S3_TOPIC_TRANSFORM_REGEX_CONFIG = "s3.topic.transform.regex";

    public static final String S3_TOPIC_TRANSFORM_REPLACEMENT_CONFIG =
        "s3.topic.transform.replacement";

    public static final String S3_SERVICE_ENDPOINT_CONFIG = "s3.service.endpoint";

    public static final String S3_REGION_CONFIG = "s3.region";

    public static final String S3_ACCESS_KEY_CONFIG = "s3.access.key";

    public static final String S3_SECRET_KEY_CONFIG = "s3.secret.key";

    public static final String S3_PATH_STYLE_ACCESS_CONFIG = "s3.path.style.access";

    public static final String S3_CREATE_BUCKET_IF_NOT_EXISTS = "s3.create.bucket.if.not.exists";
  }
}
