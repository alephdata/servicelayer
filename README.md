# servicelayer

[![servicelayer](https://github.com/alephdata/servicelayer/actions/workflows/build.yml/badge.svg)](https://github.com/alephdata/servicelayer/actions/workflows/build.yml)

:warning: PROJECT STATUS: SUNSETTING :warning:
Our involvement with this open-source project is being sunsetted. Maintenance of this version will officially end after December 2025.

**Why?**

This decision marks a significant strategic shift for us. Over the past year, our team has completely rewritten the Aleph codebase from scratch to launch Aleph Pro. As we transition to this new supported platform, we are focusing our resources entirely on Aleph Pro to ensure we can keep the lights on for investigations around the world.

For further details on this decision and what it means for the future, please read our `official FAQs <https://www.occrp.org/en/announcement/aleph-pro-frequently-asked-questions-on-the-future-of-occrps-investigative-data-platform/>`__ .

**Timeline & Support**

- We will continue to provide maintenance for this repository until December 31st, 2025. After this date, no further updates, bug fixes, or support will be provided by the core team.
- For any questions regarding the transition or the legacy software, please reach out via our `Discourse community <https://aleph.discourse.group//>`__.
- For those currently hosting their own Aleph instances, we will be in touch with you very soon regarding the transition.
- Organizations and individuals looking to collaborate can reach out to aleph-pro@occrp.org.

**Thank you!**
We are incredibly proud of what we’ve built so far. Thank you to all the contributors and community members who helped build this project and believed in our mission.

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
