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

2. Get your Endpoint, Identity, Client ID and Client Secret

  **Endpoint and Identity**

  The base URL contains the account id (a.k.a. Munchkin id) and is therefore unique for each Marketo subscription.
  Your base URL is found by logging into Marketo and navigating to the Admin > Integration > Web Services menu.
  It is labled as “Endpoint:” underneath the “REST API” section as shown in the following screenshots.

  Identity is found directly below the endpoint entry.

  http://developers.marketo.com/rest-api/base-url/

  **Client ID and Secret**

  These values are obtained by creating an app to integrate with Marketo.

  http://developers.marketo.com/rest-api/authentication/

3. Create the config file

    Create a JSON file called `config.json` containing the Endpoint, Identity, Client ID and Client Secret.

    ```json
    {"endpoint": "your-endpoint",
     "identity": "your-identity",
     "client_id": "your-client_id",
     "client_secret": "your-client-secret"}
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

## Upgrading from tap-marketo < 3.1.0

Versions of tap-marketo prior to 3.1.0 did not declare a stream-level
`replication_method`/`replication_key` in the catalog for the `activities_*`
streams. As of 3.1.0, those streams declare `INCREMENTAL` replication with an
explicit `activityDate` replication key, so destinations can properly
merge/dedupe and rely on the bookmarked replication key.

Existing connections that upgrade to 3.1.0+ keep replicating forward from
whatever `activityDate` bookmark they already have. They do **not**
automatically backfill history, which means the destination may be missing
any `activities_*` records written before the tap started tracking that
bookmark, or before the destination table was keyed/merged on the new
replication key. If your destination relies on `activityDate` for merge or
dedup behavior, that older data effectively falls outside its visibility
until it is backfilled.

To force a one-time full historical backfill of the `activities_*` streams:

1. Add `"force_activities_backfill": true` to `config.json`.
2. Run a sync. On this run the tap will:
   - Clear the existing `activities_*` bookmarks (replication-key and any
     in-progress export bookmarks), logging a warning for each stream
     reset.
   - Re-seed those bookmarks from `config["start_date"]`, causing every
     `activities_*` stream to be fully re-extracted from `start_date`
     forward.
3. **Remove `force_activities_backfill` from `config.json`** (or set it back
   to `false`) immediately after this sync completes. Leaving it set to
   `true` forces a full backfill on **every** subsequent run, which is
   wasteful and will repeatedly re-send already-synced activity history.

**Destination replay implications:** because this reprocesses the full
history of every `activities_*` stream, expect the destination to receive
(and replay/merge) every historical `activities_*` record again for the
configured `start_date` range. For most destinations this is safe because
records are keyed and deduplicated/merged on `activityDate`/the stream's
key properties, but it will temporarily increase sync duration, API usage
against Marketo, and downstream write volume. Plan the one-time backfill
sync for a low-traffic window if volume is a concern.


---

Copyright &copy; 2017 Stitch
