# servicelayer

[![servicelayer](https://github.com/alephdata/servicelayer/actions/workflows/build.yml/badge.svg)](https://github.com/alephdata/servicelayer/actions/workflows/build.yml)

Components of the aleph data toolkit needed to interact with networked services,
such as a storage archive, job queueing, cache, and structured logging. This
package contains some common configuration components for all of these services
using environment variables.

## archive mechanism

This library provides a configurable method for file storage used by aleph and
memorious. It will store files based on their content hash (SHA1) and allows for
later retrieval of the content.


## Release procedure


```
git pull --rebase
make build-docker test
bump2version --no-commit --dry-run --verbose {patch,minor,major} # to test if this looks good
bump2version --verbose {patch,minor,major}
git push --atomic origin main $(git describe --tags --abbrev=0)
```
