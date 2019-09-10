# Changelog

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
