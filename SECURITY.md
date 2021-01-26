# Security Policy

## Supported Versions

We release patches for security vulnerabilities. Which versions are eligible
receiving such patches depend on the CVSS v3.0 Rating:

| CVSS v3.0 | Supported Versions                        |
| --------- | ----------------------------------------- |
| 4.0-10.0  | Most recent release                       |

## Reporting a Vulnerability

Please report (suspected) security vulnerabilities to our **[bug bounty
program](https://hackerone.com/aiven_ltd)**. You will receive a response from
us within 2 working days. If the issue is confirmed, we will release a patch as
soon as possible depending on impact and complexity.

## Qualifying Vulnerabilities

Any reproducible vulnerability that has a severe effect on the security or
privacy of our users is likely to be in scope for the program.

We generally **aren't** interested in the following issues:
* Social engineering (e.g. phishing, vishing, smishing) attacks
* Brute force, DoS, text injection
* Missing best practices such as HTTP security headers (CSP, X-XSS, etc.),
  email (SPF/DKIM/DMARC records), SSL/TLS configuration.
* Software version disclosure / Banner identification issues / Descriptive
  error messages or headers (e.g. stack traces, application or server errors).
* Clickjacking on pages with no sensitive actions
* Theoretical vulnerabilities where you can't demonstrate a significant
  security impact with a proof of concept.
