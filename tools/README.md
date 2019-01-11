# Concord-BFT Tools #

This subdirectory has been created for tools for use with Concord-BFT. Currently, the following tool(s) are availalbe in this directory:
- GenerateConcordKeys: A tool for generating all the keys required by the replicas in a Concord cluster of given dimensions.

## GenerateConcordKeys ##

GenerateConcordKeys is a command line utility for generating the keys required by Concord cluster. It takes the dimensions of the deployment as command line parameters and outputs a keyfile for each replica. A function for reading the keyfiles back in when launching the replicas is also provided.

### Generating Keys ###

First, if you have not done so already, build Concord-BFT. See the build instructions in the top level `README.md` for Concord-BFT for instructions on installing Concord-BFT's required dependencies and building Concord-BFT. CMake has been set up so that GenerateConcordKeys will be automatically built along with the rest of Concord-BFT. Its executable will appear in the `tools` subdirectory of your Concord-BFT build.

Once you have built GenerateConcordKeys, switch to the `tools` subdirectory of your Concord-BFT build. If your build was successfull, there should be a binary for `GenerateConcordKeys`. Run it with:

`./GenerateConcordKeys -n <NUM_REPLICAS> -f <MAX_FAULTY> -o <OUTPUT_PREFIX>`

Here, `NUM_REPLICAS` should be the total number of replicas in this Concord cluster, `MAX_FAULTY` should be the maximum number of Byzantine-faulty replicas the cluster must tolerate before safety is lost, and `OUTPUT_PREFIX` should be a prefix for the output keyfiles. Due to the constraints of the SBFT algorithm for Byzantine fault tolerance, `NUM_REPLICAS` must be greater than `3 * MAX_FAULTY`. If the parameters are accepted and key generation is successful, `NUM_REPLICAS` keyfiles will be generated and output, each with a filename of the format `OUTPUT_PREFIX<i>`, where `OUTPUT_PREFIX` is as specified in the command line parameters and `<i>` is a sequential ID for the replica to which each keyfile corresponds in the range `[0, NUM_REPLICAS - 1]`, inclusive.

`GenerateConcordKeys` may take a while to run, depending on the cluster size, as key generation for public key and threshold cryptosystems is computationally expensive.

`./GenerateConcordKeys --help` can be run for a summary of command line parameters expected and options availalbe for `GenerateConcordKeys`, including any optional parameters `GenerateConcordKeys` supports, such as cryptosystem type selection.

### Using Generated Keys ###

The keyfiles output by `GenerateConcordKeys` should be distributed to the places where the replicas they correspond to will be run. In a production environment, for example, each replica should be run on a different machine, ideally with each machine in a different failure domain, and the keyfile for each replica should be distributed to the machine that will run that replica.

To load the keyfiles once they have been distributed to the replicas, a function, specifically `inputReplicaKeyfile`, declared in `tools/KeyfileIOUtils.hpp`, has been provided for reading in replica keyfiles from the on-disk format output by `GenerateConcordKeys` to a `bftEngine::ReplicaConfig` struct, which is the in-memory representation used by Concord-BFT when constructing bftEngine::Replica objects.

`inputReplicaKeyfile` takes a `std::istream` to read the keyfile from and a `bftEngine::ReplicaConfig` reference to write the keys read to. Note that `inputReplicaKeyfile` does not initialize the entire `bftEngine::ReplicaConfig`; it initializes only the cryptographic fields and those immediately related to them, such as `fVal` and `cVal`. Fields such as `statusReportTimerMillisec`, which are not related to the cryptographic configuration, are untouched by `inputReplicaKeyfile`. `inputReplicaKeyfile` returns a `bool` indicating whether it succeeded or failed; the entire `bftEngine::Replicaconfig` should be untouched in the event of a failure. The primary reason `inputReplicaKeyfile` might fail is a malformatted or otherwise invalid keyfile. Please see the comments on the declaration of `inputReplicaKeyfile` in `tools/KeyfileIOUtils.hpp` for more details about `inputReplicaKeyfile`, such as exactly which `bftEngine::ReplicaConfig` fields it sets if it is successful.

### Keyfile Format ###

The keyfiles `GenerateConcordKeys` produces and that can be read with `inputReplicaKeyfile` are of a simple format that is intended to be easily readable by both humans and software.

#### Keyfile Structure and Syntax ####

The data in a keyfile of this format is structured as an associative array, that is, a mapping of values to keys. Two fundamental types of values are supported: strings and ordered lists of strings. The strings in either of these types of values may be re-interpreted as other things (for example, integers) depending on what the keys are. The keys must also be strings. Whitespace and the characters `':'` and `'#'` are explicitly disallowed from both keys and values.

This format is insensitive to the order that these value-to-key assignments are made in. Having more than one assignment to the same key is explicitly disallowed, even if the same value is assigned both times. Additionally, keyfiles of this format may contain comments.

Comments are started with a `'#'` character and continue until the end of the line they started on; comments are to be completely ignored when parsing a keyfile; blank lines, indentation, and trailing space are also to be completely ignored. Furthermore, anywhere that a space is allowed or required, any ammount of non-newline whitespace is permitted.

The syntax for assigning a string value to a key is:

`KEY: VALUE`

where `KEY` is the key and `VALUE` is the value. The key, `:`, and value must all be on the same line, in that order, and only one such assignment is allowed per line. 

The syntax for assigning a list of strings to a key is:

```
KEY:
  - VALUE_1
  - VALUE_2
  - VALUE_3
```

where `KEY` is the key and (`VALUE_1`, `VALUE_2`, `VALUE_3`) are the entries in the list of strings. The key (with the `:`) and each list entry (with its preceding `-`, separated from the list entry's value itself by a space (or any ammount of non-newline whitespace)) must be on separate lines. Lists of any positive integer length are allowed syntactically.

The parser expects to see assignments for a specific set of keys, and it also has specific expectations about what types of values can be legally assigned to what keys. The parser will reject keyfiles if they are missing any assignments it expects to find or if any of the values assigned are illegal. The parser will also reject files containing assignments to any keys that it does not recognize.

#### Expected Contents of the Keyfiles  ####

The current set of keys the parser expects values assigned to will be grouped into four broad categories for the purpose of this discussion: fundamental parameters, a list of public RSA keys, threshold cryptosystem public configurations, and private keys for the replica to which this keyfile belongs. Parameters in the first three categories should have consistent values among every replica's keyfile; however, the private keys should differ between replicas. 

##### Fundamental Parameters #####

There are four fundamental parameters expected:

Parameter | Expected Value | Description
--- | --- | ---
`num_replicas` | positive integer | Total number of replicas for this Concord-BFT cluster. The total number of replicas must be equal to `3 * f_val + 2 * c_val + 1`.
`f_val` | positive integer | F parameter to the SBFT algorithm, that is, the maximum number of byzantine-faulty replicas that can be lost before safety is lost.
`c_val` | non-negative integer | C parameter to the SBFT algorithm, that is, the maximum number of slow, crashed, or otherwise unresponsive replicas that can be tolerated before Concord must fall back to the slow path for committing transactions.
`replica_id` | non-negative integer | Replica ID for the replica to which this keyfile belongs; replica IDs must be in the range [0, `num_replicas` - 1], inclusive, and no two replicas should have the same ID.

##### List of Public RSA Keys #####

A list of public RSA keys is expected under the identifier `rsa_public_keys`:

Parameter | Expected Value | Description
--- | --- | ---
`rsa_public_keys` | list of RSA keys in hexadecimal | A public RSA key for each replica for non-threshold cryptographic purposes. The keys should be in hexadecimal, and there should be as many keys as there are replicas. The keys should be given in ascending order of ID of the replica they belong to.

##### Threshold Cryptosystem Public Configuration #####

A set of parameters is expected for each threshold cryptosystem used by Concord-BFT (of which there are currently four). Although configuration of each cryptosystem is expected to vary, the type and purposes of their parameters given in the keyfile is consistent. The parameters for each of the cryptosystems are differentiated from each other with a prefix that specifies which cryptosystem the parameter specifies. The prefixes are:

Prefix | Cryptosystem Purpose
--- | ---
`execution` | Consensus on results of executing the transactions in a block.
`slow_commit` | Consensus on what transactions to commit in what order in a block, in the slow-path case where more than `c_val` replicas are slow, crashed, or otherwise unresponsive.
`commit` | Consensus on what transactions to commit in what order in a block in the general case.
`optimisitic_commit` | Consensus on what transactions to commit in what order in a block, in the optimistic fast-path case where all replicas are currently responsive.

For each cryptosystem, a set of six public configuration parameters is expected, which should each begin with that cryptosystem's prefix:

Parameter | Expected Value | Description
--- | --- | ---
`<PREFIX>_cryptosystem_type` | string | Names a cryptosystem type for this cryptosystem; note that a summary of available cryptosystem types is included in `GenerateConcordKeys`'s help text.
`<PREFIX>_cryptosystem_subtype_parameter` | string | A type-specific parameter specifying a subtype for this cryptosystem. For example, this is generally an elliptic curve type if an elliptic curve cryptosystem was selected.
`<PREFIX>_cryptosystem_num_signers` | positive integer | Number of signers in this cryptosystem; it is expected that this value will be equal to `num_replicas`.
`<PREFIX>_cryptosystem_threshold` | positive integrer | Threshold for this cryptosystem, that is, the number of unique signers that are required to produce a complete threshold signature under this cryptosystem.
`<PREFIX>_cryptosystem_public_key` | cryptographic key of type-dependent format | The public key for this threshold cryptosystem.
`<PREFIX>_cryptosystem_verification_keys` | list of cryptographic key of type-dependent format | A verification key for each signer under this cryptosystem. The verification keys can be used to verify a specific signer's signature share during the collection of signatures for a complete threshold signture. The keys should be ordered by signer ID of the signer to which they correspond.

##### Replica Private Keys #####

For each threshold cryptosystem, a private key is expected for the replica to which the keyfile belongs. Similarly to the threshold cryptosystem public configuration, the private keys' identifiers use a prefix to identify which cryptosystem they correspond to:

Parameter | Expected Value | Description
--- | --- | ---
`<PREFIX>_cryptosystem_private_key` | cryptographic key of a type-dependent format | The private key for the replica identified by `replica_id` under the threshold cryptosystem named by `PREFIX`.

Furthermore, a private RSA key is expected for the replica:

Parameter | Expected Value | Description
--- | --- | ---
`rsa_private_key` | RSA private key represented in hexadecimal | RSA private key for the replica identified by `replica_id`.

### Testing Key Generation ###

A utility for testing the validity of a complete set of keyfiles is provided, which can be used to test and verify the output of `GenerateConcordKeys` and could also be used to verify that a set of keyfiles is still valid after making post-generation manual or automatic adustments to them. The test is `TestGeneratedKeys`, and a binary for it should be included in the same directory (`tools`) as `GenerateConcordKeys` in a complete build of Concord-BFT. The test can be run with the following command:

`TestGeneratedKeys -n <NUM_REPLICAS> -o <OUTPUT_PREFIX>`

where `NUM_REPLICAS` and `OUTPUT_PREFIX` are the same as those that were used when `GenerateConcordKeys` was run; that is, `TestGeneratedKeys` expects to find `NUM_REPLICAS` keyfiles to test with names of the format `OUTPUT_PREFIX<i>`, where `i` is an index in the range `[0, NUM_REPLICAS - 1]`, inclusive, indicating which replica each keyfile belongs to.

The last line of `TestGeneratedKeys`'s output (to the terminal) will include `SUCCESS` if the keyfiles passed the test or `FAILURE` if they did not; furthermore, `TestGeneratedKeys` will have a zero return code for passing results and a non-zero return code for failing results.

A bash shell script, specifically `testKeyGeneration.sh`, is also available in the same directory as `GenerateConcordKeys` and `TestGeneratedKeys` to test that key generation and keyfile parsing are working correctly. `testKeyGeneration.sh` automatically runs `GenerateConcordKeys` and then tests its output with `TestGeneratedKeys` for several different sizes of Concord clusters. By default, the largest test that `testKeyGeneration.sh` offers is not run because the large test may take a while to run. In order to run `testKeyGeneration.sh` including this optional large test, run `testKeyGeneration.sh` with the `--run_large_test` option.
