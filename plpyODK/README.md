# First version of functions that automatically get data from ODK central using a filter
Central gives all datas by default. We use ODK every day to collect data that goes and is edited in our own GIS database.
Each day we download hourly a lot of data that are already consolidated into our GIS. It consumes a lot, at least too much, bandwidth and energy to run.
We can now ask central for the only data that are not already in our database, so maybe 30 or 40 submissions instead of 5000 ;-)
## Requirements
### pyODK config file
.pyodk_config.toml conf file must exists in Postgresql directory (ie /var/lib/postgresql/)

```toml
[central]
base_url = "https://my_central_server.url"
username = "my_username"
password = "my_password"
default_project_id = 5
```
### pl/python langage installed on you databse
```sql
CREATE OR REPLACE PROCEDURAL LANGUAGE plpython3u;
```
## Example