# Release Process

This project publishes Java artifacts to Maven Central from the `Release` GitHub Actions workflow.

## Prerequisites

- `gradle.properties` contains the release version, for example `version=0.8.0`.
- GitHub Actions secrets are configured:
  - `MAVEN_CENTRAL_USERNAME`
  - `MAVEN_CENTRAL_PASSWORD`
  - `GPG_PRIVATE_KEY`
  - `GPG_PRIVATE_KEY_PASSPHRASE`
  - `PAGES_DEPLOY_KEY`
- The release commit is merged to `main`.

## Create a Release

1. Verify the release version:

   ```bash
   rg '^version=' gradle.properties
   ```

2. Create and push an annotated tag from `main`:

   ```bash
   git fetch origin main
   git switch main
   git pull --ff-only origin main
   git tag -a v0.8.0 -m "Release v0.8.0"
   git push origin refs/tags/v0.8.0
   ```

3. Watch the release workflow:

   ```bash
   gh run list --workflow ci-release.yaml --branch v0.8.0 --limit 1
   gh run watch <run-id> --exit-status
   ```

The release workflow is tag-only. It publishes with `publishAndReleaseToMavenCentral`, waits for Maven Central to report `PUBLISHED`, then regenerates and publishes the latest Javadocs.

## Verify Maven Central

After the workflow succeeds, verify the public Maven Central repository:

```bash
curl -fsSL https://repo.maven.apache.org/maven2/io/github/oxia-db/oxia-client/maven-metadata.xml
curl -I -fsSL https://repo.maven.apache.org/maven2/io/github/oxia-db/oxia-client/0.8.0/oxia-client-0.8.0.jar
curl -I -fsSL https://repo.maven.apache.org/maven2/io/github/oxia-db/oxia-client-api/0.8.0/oxia-client-api-0.8.0.jar
curl -I -fsSL https://repo.maven.apache.org/maven2/io/github/oxia-db/oxia-perf/0.8.0/oxia-perf-0.8.0.jar
```

The metadata should show the release version as both `latest` and `release`, and each artifact request should return HTTP 200.

## Recovery Notes

- Do not use `publishAllPublicationsToMavenCentral` for releases. That task uploads a user-managed Central Portal deployment and can leave artifacts unpublished.
- If a release workflow uploaded but did not publish, check the Central Portal deployments page and publish or drop the pending deployment before retrying.
- Retagging a public release should be a last resort. If needed, move the annotated tag to the corrected `main` commit and force-push only the tag:

  ```bash
  git fetch origin main --tags
  git tag -fa v0.8.0 origin/main -m "Release v0.8.0"
  git push --force origin refs/tags/v0.8.0
  ```

  Confirm that the new tag-triggered workflow runs against the updated commit and then re-run the Maven Central verification commands.
