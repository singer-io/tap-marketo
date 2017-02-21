# tap-marketo

Tap for Marketo

http://developers.marketo.com/rest-api/

## Config values

### Endpoint and Identity

The base URL contains the account id (a.k.a. Munchkin id) and is therefore unique for each Marketo subscription.
Your base URL is found by logging into Marketo and navigating to the Admin > Integration > Web Services menu.
It is labled as “Endpoint:” underneath the “REST API” section as shown in the following screenshots.

Identity is found direcly below the endpoint entry.

http://developers.marketo.com/rest-api/base-url/

### Max Daily Calls

All accounts have a max number of 10000 account calls. We use 8000 as a default to allow the client to use their
API access as well. Accounts can contact Marketo to raise the limit, so this is user-definable.

### Client ID and Secret

These values are obtained by creating an app to integrate with Marketo.

http://developers.marketo.com/rest-api/authentication/


---

Copyright &copy; 2017 Stitch
