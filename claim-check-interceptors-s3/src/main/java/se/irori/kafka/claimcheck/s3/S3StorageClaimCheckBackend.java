package se.irori.kafka.claimcheck.s3;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.KafkaStorageException;
import se.irori.kafka.claimcheck.ClaimCheck;
import se.irori.kafka.claimcheck.ClaimCheckBackend;
import se.irori.kafka.claimcheck.s3.S3ClaimCheckConfig.Keys;
import se.irori.kafka.claimcheck.s3.S3ClaimCheckConfig.PartitionGranularity;

/**
 * Implementation of the ClaimCheckBackend backed by an S3 compatible object store
 * (AWS S3 or MinIO).
 *
 * <p>The claim check reference is an {@code s3://bucket/key} URI.
 */
public class S3StorageClaimCheckBackend implements ClaimCheckBackend {

  /** URI scheme used for the claim check reference. */
  public static final String URI_SCHEME = "s3";

  private static final DateTimeFormatter MONTHLY_FMT =
      DateTimeFormatter.ofPattern("yyyy/MM").withZone(ZoneOffset.UTC);
  private static final DateTimeFormatter DAILY_FMT =
      DateTimeFormatter.ofPattern("yyyy/MM/dd").withZone(ZoneOffset.UTC);
  private static final DateTimeFormatter HOURLY_FMT =
      DateTimeFormatter.ofPattern("yyyy/MM/dd/HH").withZone(ZoneOffset.UTC);

  private AmazonS3 s3client;
  private String bucketName;
  private String objectPrefix;
  private PartitionGranularity partitionGranularity;
  private Pattern topicTransformPattern;
  private String topicTransformReplacement;
  private boolean createBucketIfNotExists;

  @Override
  public ClaimCheck checkIn(ProducerRecord<byte[], byte[]> largeRecord) {
    byte[] payload = largeRecord.value();

    ObjectMetadata meta = newMetadata(payload.length);
    meta.addUserMetadata("kafka-topic", largeRecord.topic());
    if (largeRecord.partition() != null) {
      meta.addUserMetadata("kafka-partition", largeRecord.partition().toString());
    }
    if (largeRecord.timestamp() != null) {
      meta.addUserMetadata("kafka-timestamp", largeRecord.timestamp().toString());
    }

    return store(largeRecord.topic(), new ByteArrayInputStream(payload), meta);
  }

  @Override
  public ClaimCheck checkInStreaming(String topic, InputStream payload, long payloadSize) {
    ObjectMetadata meta = newMetadata(payloadSize);
    meta.addUserMetadata("kafka-topic", topic);
    return store(topic, payload, meta);
  }

  @Override
  public byte[] checkOut(ClaimCheck claimCheck) {
    try (S3Object object = getObject(claimCheck)) {
      return IOUtils.toByteArray(object.getObjectContent());
    } catch (IOException e) {
      throw new KafkaStorageException("Failed to read S3 object for claim check: "
          + claimCheck.getReference(), e);
    }
  }

  @Override
  public InputStream checkOutStreaming(ClaimCheck claimCheck) {
    return getObject(claimCheck).getObjectContent();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    S3ClaimCheckConfig config = S3ClaimCheckConfig.validatedConfig(configs);

    bucketName = config.getString(Keys.S3_BUCKET_NAME_CONFIG);
    objectPrefix = normalizePrefix(config.getString(Keys.S3_OBJECT_PREFIX_CONFIG));
    partitionGranularity = config.getPartitionGranularity();
    String topicRegex = config.getString(Keys.S3_TOPIC_TRANSFORM_REGEX_CONFIG);
    topicTransformPattern = topicRegex.isEmpty() ? null : Pattern.compile(topicRegex);
    topicTransformReplacement = config.getString(Keys.S3_TOPIC_TRANSFORM_REPLACEMENT_CONFIG);
    createBucketIfNotExists = config.getBoolean(Keys.S3_CREATE_BUCKET_IF_NOT_EXISTS);

    AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard()
        .withPathStyleAccessEnabled(config.getBoolean(Keys.S3_PATH_STYLE_ACCESS_CONFIG));

    String region = config.getString(Keys.S3_REGION_CONFIG);
    String endpoint = config.getString(Keys.S3_SERVICE_ENDPOINT_CONFIG);
    if (endpoint != null) {
      builder.withEndpointConfiguration(
          new AwsClientBuilder.EndpointConfiguration(endpoint, region));
    } else {
      builder.withRegion(region);
    }

    String accessKey = config.getString(Keys.S3_ACCESS_KEY_CONFIG);
    if (accessKey != null) {
      builder.withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(
          accessKey, config.getPassword(Keys.S3_SECRET_KEY_CONFIG).value())));
    }

    s3client = builder.build();
  }

  @Override
  public void close() {
    if (s3client != null) {
      s3client.shutdown();
    }
  }

  private ClaimCheck store(String topic, InputStream payload, ObjectMetadata meta) {
    String resolvedTopic = transformTopic(topic);
    String bucket = applyTopicPlaceholder(bucketName, resolvedTopic);
    String key = buildObjectKey(resolvedTopic);

    if (createBucketIfNotExists && !s3client.doesBucketExistV2(bucket)) {
      s3client.createBucket(bucket);
    }

    s3client.putObject(bucket, key, payload, meta);
    return new ClaimCheck(URI_SCHEME + "://" + bucket + "/" + key);
  }

  private S3Object getObject(ClaimCheck claimCheck) {
    URI ref;
    try {
      ref = URI.create(claimCheck.getReference());
    } catch (IllegalArgumentException e) {
      throw new KafkaStorageException("Bad S3 claim check uri: " + claimCheck.getReference(), e);
    }
    if (!URI_SCHEME.equals(ref.getScheme()) || ref.getHost() == null || ref.getPath() == null
        || ref.getPath().length() <= 1) {
      throw new KafkaStorageException("Bad S3 claim check uri: " + claimCheck.getReference());
    }
    return s3client.getObject(ref.getHost(), ref.getPath().substring(1));
  }

  private static ObjectMetadata newMetadata(long contentLength) {
    ObjectMetadata meta = new ObjectMetadata();
    meta.setContentLength(contentLength);
    meta.setContentType("application/octet-stream");
    meta.addUserMetadata("payload-size-bytes", Long.toString(contentLength));
    return meta;
  }

  private String buildObjectKey(String resolvedTopic) {
    StringBuilder key = new StringBuilder();
    String prefix = applyTopicPlaceholder(objectPrefix, resolvedTopic);
    if (!prefix.isEmpty()) {
      key.append(prefix).append('/');
    }
    String partition = partitionPath();
    if (!partition.isEmpty()) {
      key.append(partition).append('/');
    }
    return key.append(UUID.randomUUID()).toString();
  }

  private String transformTopic(String topic) {
    return topicTransformPattern == null
        ? topic
        : topicTransformPattern.matcher(topic).replaceAll(topicTransformReplacement);
  }

  private static String applyTopicPlaceholder(String template, String topic) {
    return template.replace(S3ClaimCheckConfig.TOPIC_PLACEHOLDER, topic);
  }

  private String partitionPath() {
    Instant now = Instant.now();
    switch (partitionGranularity) {
      case MONTHLY:
        return MONTHLY_FMT.format(now);
      case DAILY:
        return DAILY_FMT.format(now);
      case HOURLY:
        return HOURLY_FMT.format(now);
      default:
        return "";
    }
  }

  private static String normalizePrefix(String prefix) {
    String p = prefix.trim();
    while (p.startsWith("/")) {
      p = p.substring(1);
    }
    while (p.endsWith("/")) {
      p = p.substring(0, p.length() - 1);
    }
    return p;
  }
}
