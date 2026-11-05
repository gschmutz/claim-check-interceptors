package se.irori.kafka.claimcheck.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;
import se.irori.kafka.claimcheck.s3.S3ClaimCheckConfig.Keys;

/**
 * Unit test S3ClaimCheckConfig for valid config combinations.
 */
public class S3ClaimCheckConfigTest {

  @Test
  public void testEmptyConfigUsesDefaults() {
    S3ClaimCheckConfig config = S3ClaimCheckConfig.validatedConfig(new HashMap<>());

    assertEquals("large-object-bucket", config.getString(Keys.S3_BUCKET_NAME_CONFIG));
    assertEquals(S3ClaimCheckConfig.PartitionGranularity.NONE, config.getPartitionGranularity());
    assertFalse(config.getBoolean(Keys.S3_CREATE_BUCKET_IF_NOT_EXISTS));
  }

  @Test
  public void testAccessAndSecretKeyOk() {
    Map<String, String> config = new HashMap<>();
    config.put(Keys.S3_ACCESS_KEY_CONFIG, "access");
    config.put(Keys.S3_SECRET_KEY_CONFIG, "secret");

    S3ClaimCheckConfig.validatedConfig(config);
  }

  @Test(expected = ConfigException.class)
  public void testAccessKeyWithoutSecretKeyError() {
    Map<String, String> config = new HashMap<>();
    config.put(Keys.S3_ACCESS_KEY_CONFIG, "access");

    S3ClaimCheckConfig.validatedConfig(config);
  }

  @Test
  public void testPartitionGranularityCaseInsensitive() {
    Map<String, String> config = new HashMap<>();
    config.put(Keys.S3_PARTITION_GRANULARITY_CONFIG, "Daily");

    assertEquals(S3ClaimCheckConfig.PartitionGranularity.DAILY,
        S3ClaimCheckConfig.validatedConfig(config).getPartitionGranularity());
  }

  @Test(expected = ConfigException.class)
  public void testInvalidPartitionGranularityError() {
    Map<String, String> config = new HashMap<>();
    config.put(Keys.S3_PARTITION_GRANULARITY_CONFIG, "yearly");

    S3ClaimCheckConfig.validatedConfig(config);
  }

  private static Map<String, String> cfg(String... kv) {
    Map<String, String> m = new HashMap<>();
    for (int i = 0; i < kv.length; i += 2) {
      m.put(kv[i], kv[i + 1]);
    }
    return m;
  }

  @Test
  public void testProviderDerivedFromSettings() {
    assertEquals(S3ClaimCheckConfig.CredentialsProviderType.DEFAULT,
        S3ClaimCheckConfig.validatedConfig(cfg()).getCredentialsProviderType());
    assertEquals(S3ClaimCheckConfig.CredentialsProviderType.STATIC,
        S3ClaimCheckConfig.validatedConfig(cfg(Keys.S3_ACCESS_KEY_CONFIG, "a",
            Keys.S3_SECRET_KEY_CONFIG, "s")).getCredentialsProviderType());
    assertEquals(S3ClaimCheckConfig.CredentialsProviderType.PROFILE,
        S3ClaimCheckConfig.validatedConfig(cfg(Keys.S3_PROFILE_NAME_CONFIG, "dev"))
            .getCredentialsProviderType());
  }

  @Test
  public void testExplicitProviders() {
    for (String p : new String[] {"default", "PROFILE", "anonymous"}) {
      S3ClaimCheckConfig.validatedConfig(cfg(Keys.S3_CREDENTIALS_PROVIDER_CONFIG, p));
    }
    assertEquals(S3ClaimCheckConfig.CredentialsProviderType.CUSTOM,
        S3ClaimCheckConfig.validatedConfig(cfg(Keys.S3_CREDENTIALS_PROVIDER_CONFIG,
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"))
            .getCredentialsProviderType());
  }

  @Test(expected = ConfigException.class)
  public void testStaticWithoutKeysError() {
    S3ClaimCheckConfig.validatedConfig(cfg(Keys.S3_CREDENTIALS_PROVIDER_CONFIG, "static"));
  }

  @Test(expected = ConfigException.class)
  public void testKeysWithNonStaticProviderError() {
    S3ClaimCheckConfig.validatedConfig(cfg(Keys.S3_CREDENTIALS_PROVIDER_CONFIG, "default",
        Keys.S3_ACCESS_KEY_CONFIG, "a", Keys.S3_SECRET_KEY_CONFIG, "s"));
  }

  @Test(expected = ConfigException.class)
  public void testProfileNameWithOtherProviderError() {
    S3ClaimCheckConfig.validatedConfig(cfg(Keys.S3_CREDENTIALS_PROVIDER_CONFIG, "anonymous",
        Keys.S3_PROFILE_NAME_CONFIG, "dev"));
  }

  @Test(expected = ConfigException.class)
  public void testUnknownProviderClassError() {
    S3ClaimCheckConfig.validatedConfig(cfg(Keys.S3_CREDENTIALS_PROVIDER_CONFIG, "no.such.Class"));
  }

  @Test(expected = ConfigException.class)
  public void testProviderClassWrongTypeError() {
    S3ClaimCheckConfig.validatedConfig(cfg(Keys.S3_CREDENTIALS_PROVIDER_CONFIG,
        "java.lang.String"));
  }
}
