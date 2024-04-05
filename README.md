# cb-util 2.2.34

## Couchbase Utilities
Couchbase connection manager. Simplifies connecting to a Couchbase cluster and performing data and management operations.

## Installing
```
$ pip install cbcmgr
```

## API Usage
Original syntax (package is backwards compatible):
```
>>> from cbcmgr.cb_connect import CBConnect
>>> from cbcmgr.cb_management import CBManager
>>> bucket = scope = collection = "test"
>>> dbm = CBManager("127.0.0.1", "Administrator", "password", ssl=False).connect()
>>> dbm.create_bucket(bucket)
>>> dbm.create_scope(scope)
>>> dbm.create_collection(collection)
>>> dbc = CBConnect("127.0.0.1", "Administrator", "password", ssl=False).connect(bucket, scope, collection)
>>> result = dbc.cb_upsert("test::1", {"data": 1})
>>> result = dbc.cb_get("test::1")
>>> print(result)
{'data': 1}
```
New Operator syntax:
```
keyspace = "test.test.test"
db = CBOperation(hostname, "Administrator", "password", ssl=False, quota=128, create=True).connect(keyspace)
db.put_doc(col_a.collection, "test::1", document)
d = db.get_doc(col_a.collection, "test::1")
assert d == document
db.index_by_query("select data from test.test.test")
r = db.run_query(col_a.cluster, "select data from test.test.test")
assert r[0]['data'] == 'data'
```
Thread Pool Syntax:
```
pool = CBPool(hostname, "Administrator", "password", ssl=False, quota=128, create=True)
pool.connect(keyspace)
pool.dispatch(keyspace, Operation.WRITE, f"test::1", document)
pool.join()
```
Async Pool Syntax
```
pool = CBPoolAsync(hostname, "Administrator", "password", ssl=False, quota=128, create=True)
await pool.connect(keyspace)
await pool.join()
```
## CLI Utilities
## cbcutil
Load 1,000 records of data using the default schema:
````
$ cbcutil load --host couchbase.example.com --count 1000 --schema default
````
Load data from a test file:
````
$ cat data/data_file.txt | cbcutil load --host couchbase.example.com -b bucket
````
Export data from a bucket to CSV (default output file location is $HOME)
````
$ cbcutil export csv --host couchbase.example.com -i -b sample_app
````
Export data as JSON and load that data into another cluster
````
$ cbcutil export json --host source -i -O -q -b bucket | cbcutil load --host destination -b bucket
````
Get a document from a bucket using the key:
````
$ cbcutil get --host couchbase.example.com -b employees -k employees:1
````
List information about a Couchbase cluster:
````
$ cbcutil list --host couchbase.example.com -u developer -p password
````
List detailed information about a Couchbase cluster including health information:
````
$ cbcutil list --host couchbase.example.com --ping -u developer -p password
````
Replicate buckets, indexes and users from self-managed cluster to Capella (and filter buckets beginning with "test" and users with usernames beginning with "dev"):
```
cbcutil replicate source --host 1.2.3.4 --filter 'bucket:test.*' --filter 'user:dev.*' | cbcutil replicate target --host cb.abcdefg.cloud.couchbase.com -p "Password123#" --project dev-project --db testdb
```
List available schemas:
````
$ cbcutil schema
````
# Randomizer tokens
Note: Except for the US States the random data generated may not be valid. For example the first four digits of the random credit card may not represent a valid financial institution. The intent is to simulate real data. Any similarities to real data is purely coincidental.  

| Token            | Description                                                   |
|------------------|---------------------------------------------------------------|
| date_time        | Data/time string in form %Y-%m-%d %H:%M:%S                    |
| rand_credit_card | Random credit card format number                              |
| rand_ssn         | Random US Social Security format number                       |
| rand_four        | Random four digits                                            |
| rand_account     | Random 10 digit number                                        |
| rand_id          | Random 16 digit number                                        |
| rand_zip_code    | Random US Zip Code format number                              |
| rand_dollar      | Random dollar amount                                          |
| rand_hash        | Random 16 character alphanumeric string                       |
| rand_address     | Random street address                                         |
| rand_city        | Random city name                                              |
| rand_state       | Random US State name                                          |
| rand_first       | Random first name                                             |
| rand_last        | Random last name                                              |
| rand_nickname    | Random string with a concatenated first initial and last name |
| rand_email       | Random email address                                          |
| rand_username    | Random username created from a name and numbers               |
| rand_phone       | Random US style phone number                                  |
| rand_bool        | Random boolean value                                          |
| rand_year        | Random year from 1920 to present                              |
| rand_month       | Random month number                                           |
| rand_day         | Random day number                                             |
| rand_date_1      | Near term random date with slash notation                     |
| rand_date_2      | Near term random date with dash notation                      |
| rand_date_3      | Near term random date with spaces                             |
| rand_dob_1       | Date of Birth with slash notation                             |
| rand_dob_2       | Date of Birth with dash notation                              |
| rand_dob_3       | Date of Birth with spaces                                     |
| rand_image       | Random 128x128 pixel JPEG image                               |
# Options
Usage: cbcutil command options

| Command  | Description               |
|----------|---------------------------|
| load     | Load data                 |
| get      | Get data                  |
| list     | List cluster information  |
| export   | Export data               |
| import   | Import via plugin         |
| clean    | Remove buckets            |
| schema   | Schema management options |
| replicate| Replicate configuration   |

| Option                                 | Description                                                    |
|----------------------------------------|----------------------------------------------------------------|
| -u USER, --user USER                   | User Name                                                      |
| -p PASSWORD, --password PASSWORD       | User Password                                                  |
| -h HOST, --host HOST                   | Cluster Node or Domain Name                                    |
| -b BUCKET, --bucket BUCKET             | Bucket name                                                    |
| -s SCOPE, --scope SCOPE                | Scope name                                                     |
| -c COLLECTION, --collection COLLECTION | Collection name                                                |
| -k KEY, --key KEY                      | Key name or pattern                                            |
| -d DATA, --data DATA                   | Data to import                                                 |
| -F FILTER, --filter FILTER             | Filter expression (i.e. bucket:regex, user:regex, etc.)        |
| --project PROJECT                      | Capella project name                                           |
| --db DATABASE                          | Capella database name                                          |
| -q, --quiet                            | Quiet mode (only necessary output)                             |
| -O, --stdout                           | Output exported data to the terminal                           |
| -i, --index                            | Create a primary index for export operations (if not present)  |
| --tls                                  | Enable SSL (default)                                           |
| -e, --external                         | Use external network for clusters with an external network     |
| --schema SCHEMA                        | Schema name                                                    |
| --count COUNT                          | Record Count                                                   |
| --file FILE                            | File mode schema JSON file                                     |
| --id ID                                | ID field (for file mode)                                       |
| --directory DIRECTORY                  | Directory for export operations                                |
| --defer                                | Creates an index as deferred                                   |
| -P PLUGIN                              | Import plugin                                                  |
| -V PLUGIN_VARIABLE                     | Pass variable in form key=value to plugin                      |

## sgwutil
Database Commands:

| Command | Description                                 |
|---------|---------------------------------------------|
| create  | Create SGW database (connect to CBS Bucket) |
| delete  | Delete a database                           |
| sync    | Manage Sync Function for database           |
| resync  | Reprocess documents with sync function      |
| list    | List database                               |
| dump    | Dump synced document details                |

User Commands:

| Command | Description                          |
|---------|--------------------------------------|
| create  | Create users                         |
| delete  | Delete user                          |
| list    | List users                           |
| map     | Create users based on document field |

Database parameters:

| Parameter      | Description                   |
|----------------|-------------------------------|
| -b, --bucket   | Bucket                        |
| -n, --name     | Database name                 |
| -f, --function | Sync Function file            |
| -r, --replicas | Number of replicas            |
| -g, --get      | Display current Sync Function |

User parameters:

| Parameter      | Description                                           |
|----------------|-------------------------------------------------------|
| -n, --name     | Database name                                         |
| -U, --sguser   | Sync Gateway user name                                |
| -P, --sgpass   | Sync Gateway user password                            |
| -d, --dbhost   | Couchbase server connect name or IP (for map command) |
| -l, --dblogin  | Couchbase server credentials in form user:password    |
| -f, --field    | Document field to map                                 |
| -k, --keyspace | Keyspace with documents for map                       |
| -a, --all      | List all users                                        |

Examples:

Create Sync Gateway database "sgwdb" that is connected to bucket "demo":
```
sgwutil database create -h hostname -n sgwdb -b demo
```

Get information about database "sgwdb":
```
sgwutil database list -h hostname -n sgwdb
```

Display information about documents in the database including the latest channel assignment:
```
sgwutil database dump -h hostname -n sgwdb
```

Create a Sync Gateway database user:
```
sgwutil user create -h hostname -n sgwdb --sguser sgwuser --sgpass "password"
```

Display user details:
```
sgwutil user list -h hostname -n sgwdb --sguser sgwuser
```

List all database users:
```
sgwutil user list -h hostname -n sgwdb -a
```

Create users in database "sgwdb" based on the unique values for document value "field_name" in keyspace "demo":
```
sgwutil user map -h sgwhost -d cbshost -f field_name -k demo -n sgwdb
```

Add Sync Function:
```
sgwutil database sync -h hostname -n sgwdb -f /home/user/demo.js
```

Display Sync Function:
```
sgwutil database sync -h hostname -n sgwdb -g
```

Delete user:
```
sgwutil user delete -h hostname -n sgwdb --sguser sgwuser
```

Delete database "sgwdb":
```
sgwutil database delete -h hostname -n sgwdb
```
## caputil
Note: Save Capella v4 token file as $HOME/.capella/default-api-key-token.txt\
Create Capella cluster:
```
caputil cluster create --project project-name --name testdb --region us-east-1
```
Update Capella cluster (to add services):
```
caputil cluster update --project pytest-name --name testdb --services search,analytics,eventing
```
Delete Capella cluster:
```
caputil cluster delete --project project-name --name testdb --region us-east-1
```
Create bucket:
````
caputil bucket create --project project-name --db testdb --name test-bucket
````
Change database user password:
```
caputil user password --project pytest-name --db testdb --name Administrator
```
