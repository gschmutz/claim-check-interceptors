# S3 claim check Backend

Claim check backend storing large messages in an S3 compatible object store (AWS S3 or MinIO).
The claim check reference is an `s3://bucket/key` URI.

Backend class: `se.irori.kafka.claimcheck.s3.S3StorageClaimCheckBackend`

## Configuration

| Key | Default | Description |
|-----|---------|-------------|
| `s3.bucket.name` | `large-object-bucket` | Target bucket. May contain `${topic}`. |
| `s3.object.prefix` | _empty_ | Object key prefix. May contain `${topic}`. |
| `s3.partition.granularity` | `none` | `none`, `monthly`, `daily` or `hourly` (UTC date path in the key). |
| `s3.topic.transform.regex` | _empty_ | Regex applied to the topic before filling `${topic}`. |
| `s3.topic.transform.replacement` | _empty_ | Replacement used with the regex above. |
| `s3.service.endpoint` | _none_ | Endpoint, e.g. for MinIO. Uses the AWS endpoint for the region if unset. |
| `s3.region` | `us-east-1` | Signing region. |
| `s3.credentials.provider` | _auto_ | `static`, `default`, `profile`, `anonymous` or a class name implementing `AWSCredentialsProvider` (public no-arg constructor). Auto: `static` if keys are set, `profile` if `s3.profile.name` is set, else `default`. |
| `s3.access.key` / `s3.secret.key` | _none_ | Static credentials, set both. Only valid with `static`. |
| `s3.profile.name` | _none_ | Profile in `~/.aws/credentials`. Only valid with `profile` (default profile if unset). |
| `s3.path.style.access` | `true` | Path style bucket access (needed for MinIO). |
| `s3.create.bucket.if.not.exists` | `false` | Create the bucket if missing. |

Object key layout: `[prefix/][yyyy/MM/dd/HH/]<uuid>`.

## Credentials

The `default` provider uses the AWS default chain: environment variables, JVM system properties,
web identity (EKS IRSA), shared profile, ECS container and EC2 instance credentials. Prefer IAM
roles on AWS. To keep secrets out of properties files, use a Kafka `ConfigProvider`, e.g.
`s3.secret.key=${env:S3_SECRET_KEY}`.

## Object expiry

Use an S3 lifecycle rule on the bucket to expire claim check objects, for example after 14 days.
