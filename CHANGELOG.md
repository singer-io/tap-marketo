# Changelog

## 2.6.4
  * Bump dependency versions for twistlock compliance [#99](https://github.com/singer-io/tap-marketo/pull/99)

## 2.6.3
  * Removes `inclusion : automatic` from stream level catalog metadata [#98](https://github.com/singer-io/tap-marketo/pull/98)

## 2.6.2
  * Move test dependencies to `extras_require` and pin them [#96](https://github.com/singer-io/tap-marketo/pull/96)

## 2.6.1
  * Log Corona warning only if the leads and activities_* stream are selected [#94](https://github.com/singer-io/tap-marketo/pull/94)

## 2.6.0
  * Updates to run on python 3.11 [#91](https://github.com/singer-io/tap-marketo/pull/91)

## 2.5.2
  * Fixed singer logging error in V2.5.1 [#86](https://github.com/singer-io/tap-marketo/pull/86)

## 2.5.1
  * Fixed Validation Error for `max_daily_calls` [#84](https://github.com/singer-io/tap-marketo/pull/84)

## 2.5.0
  * Add campaignId field to activities streams [#82](https://github.com/singer-io/tap-marketo/pull/82)

## 2.4.4
  * Implement Request TimeOut [#78](https://github.com/singer-io/tap-marketo/pull/78)

## 2.4.3
  * Remove CR characters as CSV chunks are being written [#73](https://github.com/singer-io/tap-marketo/pull/73)

## 2.4.2
  * Ignore null characters in CSV files [#70](https://github.com/singer-io/tap-marketo/pull/70)

## 2.4.1
  * table-key-properties metadata should be a list [#68](https://github.com/singer-io/tap-marketo/pull/68)

## 2.4.0
  * Adds table-key-properties metadata [#67](https://github.com/singer-io/tap-marketo/pull/67)

## 2.3.0
  * Enables `leads` stream to work with optional parameter `max_export_days`[#65](https://github.com/singer-io/tap-marketo/pull/65)

## 2.2.9
  * Fix export availability result around issue where Marketo reports an export as existing, but returns a 404 on the underlying file. [#62](https://github.com/singer-io/tap-marketo/pull/62)

## 2.2.8
  * Alters requests backoff to a more predictable pattern that covers the rate limit window [#61](https://github.com/singer-io/tap-marketo/pull/61)
  * Increases `singer-python` dependency to `5.9.0` [#61](https://github.com/singer-io/tap-marketo/pull/61)
  * Adds explicit `backoff` dependency at version `1.8.0` [#61](https://github.com/singer-io/tap-marketo/pull/61)

## 2.2.7
  * On requests, client will retry when hitting the 100 requests per 20 seconds rate limit error [#60](https://github.com/singer-io/tap-marketo/pull/60)

## 2.2.6
  * Check for empty list instead of empty tuple for stream metadata [#58](https://github.com/singer-io/tap-marketo/pull/58)

## 2.2.5
  * Use `singer-python` functions to do stream selection

## 2.2.4
  * Use get to access dict key [commit](https://github.com/singer-io/tap-marketo/commit/2f6cb5ea278077bbf4fd73efa79faf0e0aa87cb1)
## 2.2.3
  * Ignore stream metadata when building formatting fields [commit](https://github.com/singer-io/tap-marketo/commit/afad72a975a0df8834a1a647cef4271e1845a874)

## 2.2.2
  * Ignore stream metadata when building field list [commit](https://github.com/singer-io/tap-marketo/commit/76fecfdd6289b578a041434d5d7929bb73098f36)

## 2.2.1
  * Fixed invalid json schema in `activity_types` schema [#55](https://github.com/singer-io/tap-marketo/pull/55), [#56](https://github.com/singer-io/tap-marketo/pull/56)

## 2.2.0
  * Replaced `annotated_schema` with Singer `metadata` [#54](https://github.com/singer-io/tap-marketo/pull/54)
    * Fixed unit tests to also use `metadata`
  * Added unittest for `validate_state`

## 2.1.0
  * Allows activities reports to request a value lower than the default 30 days for exports [#44](https://github.com/singer-io/tap-marketo/pull/44)

## 2.0.25
  * Update version of `requests` to `2.20.0` in response to CVE 2018-18074

## 2.0.24
  * Allow auto-discovered array values to be integer, number, string, or null

## 2.0.23
  * If a field is marked as an integer, and is already `int` type, do no conversion. (Fixes case from 2.0.22)

## 2.0.22
  * Drops decimal points for fields marked as integers and logs a warning [#42](https://github.com/singer-io/tap-marketo/pull/42)
