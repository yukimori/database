# Simple Database
Assumptions
-----------
The code only processes commands of the following type:
* `begin`
* `end`
* `commit`
* `rollback`
* `set name value`
* `unset name`
* `get name`
* `numequalto num`

Dependencies
------------
The only dependency this code relies on is the `nose` library to run tests.

Setup Instructions
------------------

1. You don't have to activate the virtual environment if you're not intending to run the tests. However, if you _do_ intend to run the tests, then you'll need to have `pip` installed locally. Once you have `pip`

+ `pip install virtualenv`
+ `virtualenv .venv`
+ `source .venv/bin/activate`
+ `pip install -r requirements.txt`

2. To run the `redis_client` from the command line, `python redis_client.py`. Start entering the commands after starting the script. The expected output will be printed out to stdout. Enter `end` to exit the program. Alternatively, if you want the code to read input from a file, run the script the following way: `python redis_client.py name_of_file`. Please make sure that the file is in the same directory as the redis_client. 
3. To run the tests, `nosetests -sv`.
3. To deactivate the virtual environment, `deactivate`. 

Implementation Details
----------------------

1. The code uses the `KeyValueStore` as the basic building block to implement several components. The `KeyValueStore` exposes a very simple and intuitive (and easy to test!) API which is used to implement 3 separate parts of the system:

+ It's used as the main in memory cache. 
+ It's also used to store the `value_cache`, which can be thought of as an inverted index, where the keys are the values used in `set` and `unset` operations, and the values associated are a set of keys. This ensures O(1) lookups for `NUMEQUALTO` operations.
+ It's also used to represent a single transaction. When used for this purpose, the keys are the variables which are a part of the transaction in question, and the value associated with each variable is of the format `{prev: val1, cur: val2}`. So, if a command `set a 100` belongs to a transaction, the transaction would be represented as follows: `{'a': {'prev': None, 'cur': 100}}`. The `prev` field stores the value of the variable set by a *previous* transaction, if one exists, else it stores `None`. The `cur` field stores the value of the variable set by the latest operation of the current open transaction. An `unset` operation would set the `cur` value to `None`. Idempotent operations such as `get` and `numequalto` don't get recorded in the write ahead log.
The reason behind using `prev` and `cur` is to make rollbacks easy. Rolling back a transaction simply entails iterating over the variables in the transaction and setting its value in the main cache to the `prev` value. Since a large number of transactions involve a small number of variables, the memory footprint of having to store the `prev` and `cur` values will be small.

2. The second component of the system is the `WriteAheadLog`. This is simply a list of all open transactions, though in a real world scenario, it'd also be used to flush changes to persistent memory, among other things. The class `WriteAheadLog`, like the `KeyValueStore`, presents a very simple and easy to use (and test) API for all transaction related operations.
The only functionality here that might require some explanation is the `_add_new_transaction` method, which is called when performing `set` and `unset` operations. Every time, it is sent a `prev` value, along with a `key` and a `value`. There are two possibilities here - either the key could've been one which has never been seen before in the current transaction (an example transaction where this is the case: `begin`, `set a 44`), or it _has_ been seen in a previous command of the current transaction (for example, `begin`, `set a 55`, `unset a`, `set a 99`). In the former case, the `prev` value is the value associated with the key in one of the previous transactions, and the value to which the key _will have to be set in case we want to rollback the current transaction_. In the second case, the `prev` value is just a value that the variable has been set to by another command belonging to the _current transaction_, and thus, can be ignored, since a previous command of the current transaction which first added the key would've already set the `prev` value. This is what lines 55-59 do of the `write_ahead_log.py` file do. 
3. The last component is the main server itself, which implements the data and transaction commands in the class `RedisServer`. The `set` and `unset` operations first make changes to the write ahead log, and then update the main cache and value cache accordingly. Rollbacks involve fetching the last transaction from the write ahead log, iterating through the variables and updating their values in the main cache and the value_cache using the value in the `prev` field.

Time/Memory Complexity
----------------------

1. `get`, `set`, `unset` and `numequalto` all take O(1) time, since the updates required are performed to the `KeyValueStore` data structure implementing the main cache, value cache and the write ahead log. All operations defined in the `KeyValueStore` take O(1) time.
2. Since a large number of transactions are only going to touch a small number of variables, there's only going to be a small number of variables in each transaction, and thus storing the `prev` field isn't going to be too expensive. Furthermore, it makes rollbacks easy. 