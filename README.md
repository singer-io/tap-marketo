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


---

Copyright &copy; 2017 Stitch
