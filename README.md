# tap-marketo

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

This tap:
- Pulls raw data from Marketo's [REST API](http://developers.marketo.com/rest-api/)
- Extracts the following resources from Marketo
  - Activity types
  - Activities
  - Leads
  - Lists
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Quick start

1. Install

    ```bash
    > pip install tap-marketo
    ```

2. Get your Endpoint, Client ID and Client Secret

  **Endpoint**

  The base URL contains the account id (a.k.a. Munchkin id) and is therefore unique for each Marketo subscription.
  Your base URL is found by logging into Marketo and navigating to the Admin > Integration > Web Services menu.
  It is labled as “Endpoint:” underneath the “REST API” section as shown in the following screenshots.

  http://developers.marketo.com/rest-api/base-url/

  **Client ID and Secret**

  These values are obtained by creating an app to integrate with Marketo.

  http://developers.marketo.com/rest-api/authentication/

3. Create the config file

    Create a JSON file called `config.json` containing the Endpoint, Start Date, Client ID and Client Secret (and optionally Attribution Window)

    **Start Date**

    Determines how much historical data will be extracted. Please be aware that the larger the time period and amount of data, the longer the initial extraction can be expected to take.

    **Attribution Window**

    [Optional] Attribution window is used to set an earlier export_start
    for incremental replication of of the **leads** stream. This allows the tap to _catch_
    leads that may have been missed in the prior export.

    `attribution_window` may be specified by a combination of days, hours and minutes. this parameter is quite useful in a moderate frequency incremental bulk extracts (e.g. once an hour) to allow users a way to avoid extracting all leads updated 1 day prior (i.e. default attribution window)
    examples of valid attribution_windows:
      * 1d
      * 12h
      * 1h30m
      * 1d6h55m

    attribution_window defaults to 1 Day if not specified.
    ```json
    {"endpoint": "your-endpoint",
     "start_date": "earliest-date-to-sync",
     "client_id": "your-client_id",
     "client_secret": "your-client-secret",
     "attribution_window": "buffer-time-subtracted-from-updatedAt-for-leads-stream"}
    ```

4. [Optional] Create the initial state file

    You can provide JSON file that contains a date for the API endpoints
    to force the application to only fetch data newer than those dates.
    If you omit the file it will fetch all Marketo data.

    ```json
    {"activity_types": "2017-01-01T00:00:00Z",
     "activities": "2017-01-01T00:00:00Z",
     "leads": "2017-01-01T00:00:00Z",
     "lists": "2017-01-01T00:00:00Z"}
    ```

5. Run the application

    `tap-marketo` can be run with:

    ```bash
    tap-marketo --config config.json [--state state.json]
    ```

## Config Parameters

| parameter name | description | examples | required or optional |
| ------ | ------ | ------ | ------ |
| endpoint | Base URL for the rest api, specific to your Marketo account (as in 123-ABC-123 would be your marketo account id in the example to the right) | https://123-ABC-123.mktorest.com/rest | required |
| start_date | the earliest date to use as filter for the replication key during the initial sync or a full refresh | 2020-01-01T00:00:00Z | required |
| client_id | The client id used to authenticate with the marketo rest api, generated through their web UI by your marketo admin (random dash separated alpha-numeric string) | a134dakfj-kldjk-39487fh3-ad834bi30 (note: actual length may differ) | required |
| client_secret | The client secret used to authenticate with the marketo rest api generated through their web UI by your marketo admin (random alpha-numeric string) | akdj498abalj314klja934 (note: actual length may differ) | required |
| attribution_window | a string specifying a duration of time (combination of days, hours & minutes) to subtract from the latest `updatedAt` value for the leads stream stored in the state | 1d, 10h, 1d1h, 1d1h30m, 20m | optional |
---

Copyright &copy; 2017 Stitch
