// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include <fstream>
#include <iostream>
#include <regex>
#include <string>
#include <unordered_map>
#include <string.hpp>
#include <exception>
#include "KeyfileIOUtils.hpp"

// Helper function to outputReplicaKeyfile.
static void serializeCryptosystemPublicConfiguration(std::ostream& output,
                                                     const Cryptosystem& system,
                                                     const std::string& sysName,
                                                     const std::string& prefix) {
  uint16_t numReplicas = system.getNumSigners();

  output << "\n# " << sysName
         << " threshold cryptosystem public"
            " configuration.\n";
  output << prefix << "_cryptosystem_type: " << system.getType() << "\n";
  output << prefix << "_cryptosystem_subtype_parameter: " << system.getSubtype() << "\n";
  output << prefix << "_cryptosystem_num_signers: " << numReplicas << "\n";
  output << prefix << "_cryptosystem_threshold: " << system.getThreshold() << "\n";
  output << prefix << "_cryptosystem_public_key: " << system.getSystemPublicKey() << "\n";

  std::vector<std::string> verificationKeys = system.getSystemVerificationKeys();

  output << prefix << "_cryptosystem_verification_keys:\n";
  for (uint16_t i = 1; i <= numReplicas; ++i) {
    output << "  - " << verificationKeys[i] << "\n";
  }
}

void outputReplicaKeyfile(uint16_t numReplicas,
                          uint16_t numRoReplicas,
                          bftEngine::ReplicaConfig& config,
                          const std::string& outputFilename,
                          Cryptosystem* execSys,
                          Cryptosystem* slowSys,
                          Cryptosystem* commitSys,
                          Cryptosystem* optSys) {

  std::ofstream output (outputFilename);
  if ((3 * config.fVal + 2 * config.cVal + 1) != numReplicas)
     throw std::runtime_error("F, C, and number of replicas do not agree for requested output.");

   output << "# Concord-BFT replica keyfile " << outputFilename << ".\n"
          << "# For replica " << config.replicaId << " in a " << numReplicas << "-replica + " << numRoReplicas << "-read-only-replica cluster.\n\n"
          << "num_replicas: " << numReplicas << "\n"
          << "num_ro_replicas: " << numRoReplicas << "\n"
          << "f_val: " << config.fVal << "\n"
          << "c_val: " << config.cVal << "\n"
          << "replica_id: " << config.replicaId << "\n"
          << "read-only: " << config.isReadOnly << "\n\n"
          << "# RSA non-threshold replica public keys\n"
          << "rsa_public_keys:\n";

   for (auto & v : config.publicKeysOfReplicas)
     output << "  - " << v.second << "\n";

   output << "\n# Private keys for this replica\n"
          << "rsa_private_key: " << config.replicaPrivateKey << "\n";

   if (execSys && slowSys && commitSys && optSys){
     serializeCryptosystemPublicConfiguration(output, *execSys, "Execution", "execution");
     serializeCryptosystemPublicConfiguration(output, *slowSys, "Slow path commit", "slow_commit");
     serializeCryptosystemPublicConfiguration(output, *commitSys, "Commit", "commit");
     serializeCryptosystemPublicConfiguration(output, *optSys, "Optimistic fast path commit", "optimistic_commit");

     output << "execution_cryptosystem_private_key: " << execSys->getPrivateKey(config.replicaId + 1) << "\n"
            << "slow_commit_cryptosystem_private_key: " << slowSys->getPrivateKey(config.replicaId + 1) << "\n"
            << "commit_cryptosystem_private_key: " << commitSys->getPrivateKey(config.replicaId + 1) << "\n"
            << "optimistic_commit_cryptosystem_private_key: " << optSys->getPrivateKey(config.replicaId + 1) << "\n";
  }
}

// Handles inputing unsigned 16-bit integers from the identifier->value map
// created while parsing the keyfile. This function validates that the desired
// integer is in the map, is a valid integer and not some other string value,
// and is within range.
template<typename T>
T parse(const std::string& identifier,
        std::unordered_map<std::string, std::string>& map,
        const std::string& filename,
        const std::unordered_map<std::string, size_t>& lineNumbers,
        bool optional = false) {
  if (map.count(identifier) < 1) {
    if(optional)
      return 0;
    if (lineNumbers.count(identifier) < 1)
      throw std::runtime_error( filename + std::string( ": Missing assignment for required parameter: ") + identifier);
    else
      throw std::runtime_error( filename + std::string(": line ") + std::to_string(lineNumbers.at(identifier)) +
                                std::string( ": expected integer value for ")  + identifier);
  }

  std::string strVal = map[identifier];
  map.erase(identifier);
  try{
    return concord::util::to<T>(strVal);
  }catch(std::exception& e){
    std::ostringstream oss;
    oss << "Exception: " << e.what() << " " << filename + ":" << lineNumbers.at(identifier) << ": invalid value for "
        << identifier << ": "<< strVal << " expected range [" << std::numeric_limits<T>::min() << ", "
        << std::numeric_limits<T>::max();

    throw std::runtime_error(oss.str());
  }
}

// Simple validators for RSA keys to ensure they at least conform to the
// expected format.
const size_t rsaPublicKeyHexadecimalLength = 584;

static bool validateRSAPublicKey(const std::string& key) {
  return (key.length() == rsaPublicKeyHexadecimalLength) && (std::regex_match(key, std::regex("[0-9A-Fa-f]+")));
}

static bool validateRSAPrivateKey(const std::string& key) {
  // Note we do not verify the length of RSA private keys because their length
  // actually seems to vary a little in the output; it hovers around 2430
  // characters but often does not exactly match that number.

  return std::regex_match(key, std::regex("[0-9A-Fa-f]+"));
}

// Checks that there are entries for all identifiers given in entries in the
// variable map produced by parsing the keyfile, and prints errors if there
// are not.
static bool expectEntries(const std::vector<std::string>& entries,
                          const std::unordered_map<std::string, std::string> map,
                          const std::string& filename,
                          const std::unordered_map<std::string, size_t> lineNumbers) {
  for (auto entry : entries) {
    if (map.count(entry) < 1) {
      if (lineNumbers.count(entry) < 1) {
        std::cout << filename << ": Missing assignment for required parameter: " << entry << ".\n";
      } else {
        std::cout << filename << ": line " << lineNumbers.at(entry) << ": expected string value for " << entry
                  << ", found list.\n";
      }
      return false;
    }
  }
  return true;
}

// Handles reading all the public configuration for each of the four
// cryptosystems we expect to read in.
static bool deserializeCryptosystemPublicConfiguration(
    std::unique_ptr<Cryptosystem>& cryptosystem,
    const std::string& name,
    const std::string& prefix,
    std::unordered_map<std::string, std::string>& valueAssignments,
    std::unordered_map<std::string, std::vector<std::string>>& listAssignments,
    const std::string& filename,
    const std::unordered_map<std::string, size_t>& identifierLines,
    const std::unordered_map<std::string, std::vector<size_t>>& listEntryLines,
    uint16_t numReplicas) {
  std::string typeVar = prefix + "_cryptosystem_type";
  std::string subtypeVar = prefix + "_cryptosystem_subtype_parameter";
  std::string numSignersVar = prefix + "_cryptosystem_num_signers";
  std::string threshVar = prefix + "_cryptosystem_threshold";
  std::string pubKeyVar = prefix + "_cryptosystem_public_key";
  std::string verifKeyVar = prefix + "_cryptosystem_verification_keys";

  uint16_t numSigners = parse<std::uint16_t>(numSignersVar, valueAssignments, filename, identifierLines);
  uint16_t threshold  = parse<std::uint16_t>(threshVar, valueAssignments, filename, identifierLines);

  if (numSigners != numReplicas) {
    std::cout << filename << ": line " << identifierLines.at(numSignersVar)
              << ": Unexpected number of signers; it is expected the number of signers"
                 " for each cryptosystem will be equal to num_replicas.\n";
    return false;
  }

  if (!expectEntries({typeVar, subtypeVar, pubKeyVar}, valueAssignments, filename, identifierLines)) {
    return false;
  }

  std::string type = valueAssignments[typeVar];
  valueAssignments.erase(typeVar);
  std::string subtype = valueAssignments[subtypeVar];
  valueAssignments.erase(subtypeVar);

  if (!Cryptosystem::isValidCryptosystemSelection(type, subtype, numSigners, threshold)) {
    std::cout << filename << ": line " << identifierLines.at(typeVar)
              << ":"
                 " Invalid configuration for "
              << name << " cryptosystem (type " << type << ", subtype " << subtype << ", threshold of " << threshold
              << " out of " << numSigners << ").\n";
    return false;
  }
  cryptosystem.reset(new Cryptosystem(type, subtype, numSigners, threshold));

  std::string publicKey = valueAssignments[pubKeyVar];
  valueAssignments.erase(pubKeyVar);

  if (!cryptosystem->isValidPublicKey(publicKey)) {
    std::cout << filename << ": line " << identifierLines.at(pubKeyVar)
              << ":"
                 " Invalid public key for selected type of cryptosystem.\n";
    return false;
  }

  if (listAssignments.count(verifKeyVar) < 1) {
    if (identifierLines.count(verifKeyVar) < 1) {
      std::cout << filename << ": Missing assignment for required parameter: " << verifKeyVar << ".\n";
    } else {
      std::cout << filename << ": line " << identifierLines.at(verifKeyVar) << ": expected list for " << verifKeyVar
                << ", found single value.\n";
    }
    return false;
  }

  std::vector<std::string> verificationKeys = std::move(listAssignments[verifKeyVar]);
  listAssignments.erase(verifKeyVar);

  // Account for convention of 1-indexing threshold signer IDs.
  verificationKeys.insert(verificationKeys.begin(), "");

  if (verificationKeys.size() != static_cast<uint16_t>(numSigners + 1)) {
    std::cout << filename << ": line " << identifierLines.at(verifKeyVar)
              << ": Unexpected number of verification keys for " << name
              << " cryptosystem; it is expected that the number of verification keys"
                 " is equal to "
              << numSignersVar << ".\n";
    return false;
  }
  for (size_t i = 1; i <= numSigners; ++i) {
    if (!cryptosystem->isValidVerificationKey(verificationKeys[i])) {
      std::cout << filename << ": line " << listEntryLines.at(verifKeyVar).at(i)
                << ": Invalid verification key for this cryptosystem.\n";
      return false;
    }
  }

  cryptosystem->loadKeys(publicKey, verificationKeys);
  return true;
}

// Generate a generic message to report any unrecognized or invalid syntax.
// Attempts to include a brief description of what syntaxes are valid.
static std::string getBadSyntaxMessage(const std::string& filename, size_t lineNumber) {
  return filename + ": line " + std::to_string(lineNumber) +
         ": Unrecognized syntax.\nRecognized syntaxes are:\n"
         "  IDENTIFIER: VALUE\nand\n"
         "  IDENTIFIER:\n    - LIST_ENTRY\n    - LIST_ENTRY\n    ...\n";
}

// Parse the input keyfile from text into a more convenient in-memory
// representations of maps from identifiers to their values. Also creates some
// records of what line numbers everything it parses come from for use in
// giving error messages referencing specific lines.
bool parseReplicaKeyfile(const std::string& filename,
                         std::unordered_map<std::string, std::string>& valueAssignments,
                         std::unordered_map<std::string, std::vector<std::string>>& listAssignments,
                         std::unordered_map<std::string, size_t>& identifierLines,
                         std::unordered_map<std::string, std::vector<size_t>>& listEntryLines) {
  std::vector<std::string>* currentList = nullptr;
  std::vector<size_t>* currentListLines = nullptr;
  std::ifstream input(filename);
  if(!input.is_open())
    throw std::runtime_error(std::string("failed to open ") + filename);
  size_t lineNumber = 1;
//  while (input.peek() != EOF) {
//    std::string line;
//    std::getline(input, line);
  for (std::string line; std::getline(input, line); ) {

    // Ignore comments.
    size_t commentStart = line.find_first_of("#");
    if (commentStart != std::string::npos) {
      line = line.substr(0, commentStart);
    }

    concord::util::trim_inplace(line);

    // Ignore this line if it contains only whitespace and/or comments.
    if (line.length() < 1) {
      ++lineNumber;
      continue;
    }

    // Note the key file format currently permits 3 types of non-empty lines:
    // assignments of values to an identifier, assignments of lists to an
    // identifier, and list entries.

    // Multiple :s are never expected in one line, so we will reject lines like
    // this here so that we do not have to handle them in both assignment cases.
    if (line.find_first_of(":") != line.find_last_of(":")) {
      std::cout << getBadSyntaxMessage(filename, lineNumber);
      return false;
    }

    // Case of a list entry. Format:
    // - LIST_ENTRY
    if ((line.length() > 2) && (line[0] == '-') && (line[1] == ' ')) {
      std::string value = concord::util::trim(line.substr(2, line.length() - 2));
      if (value.find_first_of(" \t") != std::string::npos) {
        std::cout << filename << ": line " << lineNumber
                  << ": Whitespace is"
                     " not allowed in identifiers or values.\n";
        return false;
      }

      // Note we do not to check whether the value is missing completely because
      // we know this line, beginning with "- ", had a length of 3 before
      // trimming.

      // We will check that there is not a colon in the value because the use of
      // those is reserved to indicate assignments.
      if (value.find_first_of(":") != std::string::npos) {
        std::cout << filename << ": line " << lineNumber
                  << ": Invalid list"
                     " entry: contains \":\".\n";
        return false;
      }

      if (!currentList) {
        std::cout << filename << ": line " << lineNumber
                  << ": Unexpected list"
                     " entry (not following a declaration of a list or another list"
                     " entry).\n";
        return false;
      }

      currentList->push_back(value);
      currentListLines->push_back(lineNumber);

      // Case of assignment of a list to an identifier. Format:
      // IDENTIFIER:
    } else if ((line.length() > 1) && (line[line.length() - 1] == ':')) {
      std::string identifier = concord::util::trim(line.substr(0, line.length() - 1));
      if (identifier.find_first_of(" \t") != std::string::npos) {
        std::cout << filename << ": line " << lineNumber
                  << ": Whitespace is"
                     " not allowed in identifiers or values.\n";
        return false;
      }

      // Note that we do not have to check whether the identifier is not missing
      // completely because we know the line was at least 2 characters long and
      // ended in the colon after trimming.

      if (identifierLines.count(identifier) > 0) {
        std::cout << filename << ": line " << lineNumber
                  << ": Attempting to"
                     " make a new assignment to an identifier that is already in use"
                     " (previous assignment on line "
                  << identifierLines[identifier] << ").\n";
        return false;
      }

      listAssignments[identifier] = std::vector<std::string>();
      listEntryLines[identifier] = std::vector<size_t>();

      currentList = &(listAssignments[identifier]);
      currentListLines = &(listEntryLines[identifier]);

      identifierLines[identifier] = lineNumber;

      // Case of assignment of a value to an identifier. Format:
      // IDENTIFIER: VALUE
    } else if ((line.length() >= 3) && (line.find_first_of(":") != std::string::npos)) {
      size_t colonLoc = line.find_first_of(":");
      std::string identifier = concord::util::trim(line.substr(0, colonLoc));
      std::string value = concord::util::trim(line.substr(colonLoc + 1, line.length() - (colonLoc + 1)));

      if ((identifier.find_first_of(" \t") != std::string::npos) || (value.find_first_of(" \t") != std::string::npos)) {
        std::cout << filename << ": line " << lineNumber
                  << ": Whitespace is"
                     " not allowed in identifiers or values.\n";
        std::cout << "\"" << identifier << "\"\n\"" << value << "\"\n";
        return false;
      }

      if (identifier.length() < 1) {
        std::cout << filename << ": line " << lineNumber
                  << ": Expected"
                     " identifier before assignment.\n";
        return false;
      }

      // Note we do not need to check that the value is non-empty because any
      // case that would yield that would have triggered the above case for
      // assignment of a list to an identifier.

      if (identifierLines.count(identifier) > 0) {
        std::cout << filename << ": line " << lineNumber
                  << ": Attempting to"
                     " make a new assignment to an identifier that is already in use"
                     " (previous assignment on line "
                  << identifierLines[identifier] << ").\n";
        return false;
      }

      currentList = nullptr;
      currentListLines = nullptr;

      valueAssignments[identifier] = value;
      identifierLines[identifier] = lineNumber;

      // If none of the above cases were true, then whatever this line is is not
      // of a supported format.
    } else {
      std::cout << getBadSyntaxMessage(filename, lineNumber);
      return false;
    }

    ++lineNumber;
  }

  return true;
}

bool inputReplicaKeyfile( const std::string& filename, bftEngine::ReplicaConfig& config) {
  std::unordered_map<std::string, std::string> valueAssignments;
  std::unordered_map<std::string, std::vector<std::string>> listAssignments;
  std::unordered_map<std::string, size_t> identifierLines;
  std::unordered_map<std::string, std::vector<size_t>> listEntryLines;

  if (!parseReplicaKeyfile(filename, valueAssignments, listAssignments, identifierLines, listEntryLines)) {
    return false;
  }

  std::uint16_t numReplicas =  parse<std::uint16_t>("num_replicas", valueAssignments, filename, identifierLines);
  config.fVal = parse<std::uint16_t>("f_val", valueAssignments, filename, identifierLines);
  config.cVal = parse<std::uint16_t>("c_val", valueAssignments, filename, identifierLines);
  config.replicaId = parse<std::uint16_t>("replica_id", valueAssignments, filename, identifierLines);
  //optional for backward compatibility
  config.isReadOnly = parse<bool>("read-only", valueAssignments, filename, identifierLines, true);
  config.numRoReplicas =  parse<std::uint16_t>("num_ro_replicas", valueAssignments, filename, identifierLines, true);

  // Note we validate the number of replicas using 32-bit integers in case
  // (3 * f + 2 * c + 1) overflows a 16-bit integer.
  uint32_t predictedNumReplicas = 3 * (uint32_t)config.fVal + 2 * (uint32_t)config.cVal + 1;
  if (predictedNumReplicas != (uint32_t)numReplicas) {
    std::cout << filename << ": line " << identifierLines["num_replicas"]
              << ": num_replicas must be equal to (3 * f_val + 2 * c_val + 1).\n";
    return false;
  }
  if (config.replicaId >= numReplicas + config.numRoReplicas) {
    std::cout << filename << ": line " << identifierLines["replica_id"]
              << ": invalid replica_id; replica IDs must be in the range [0,"
                 " num_replicas + num_ro_replicas].\n";
  }

  // Load RSA public keys
  if (listAssignments.count("rsa_public_keys") < 1) {
    if (identifierLines.count("rsa_public_keys") < 1) {
      std::cout << filename
                << ": Missing assignment for required parameter:"
                   " rsa_public_keys.\n";
    } else {
      std::cout << filename << ": line " << identifierLines["rsa_public_keys"]
                << ": expected list for rsa_public_keys, found single value.\n";
    }
    return false;
  }

  std::vector<std::string> rsaPublicKeys = std::move(listAssignments["rsa_public_keys"]);
  listAssignments.erase("rsa_public_keys");

  if (rsaPublicKeys.size() != numReplicas+ config.numRoReplicas) {
    std::cout << filename << ": line " << identifierLines["rsa_public_keys"]
              << ": incorrect number of public RSA keys given; the number of RSA keys"
                 " must match num_replicas.\n";
    return false;
  }
  for (size_t i = 0; i < numReplicas + config.numRoReplicas; ++i) {
    if (!validateRSAPublicKey(rsaPublicKeys[i])) {
      std::cout << filename << ": line " << listEntryLines["rsa_public_keys"][i] << ": Invalid RSA public key.\n";
      return false;
    }
  }

  // Load private keys for this replica.
  if (!expectEntries({"rsa_private_key"},
                     valueAssignments,
                     filename,
                     identifierLines)) {
      return false;
  }
  std::string rsaPrivateKey = valueAssignments["rsa_private_key"];
  valueAssignments.erase("rsa_private_key");

  if (!validateRSAPrivateKey(rsaPrivateKey)) {
    std::cout << filename << ": line " << identifierLines["rsa_private_key"] << ": Invalid RSA private key.\n";
    return false;
  }
  config.publicKeysOfReplicas.clear();
  for (uint16_t i = 0; i < numReplicas + config.numRoReplicas; ++i)
    config.publicKeysOfReplicas.insert(std::pair<uint16_t, std::string>(i, rsaPublicKeys[i]));

  config.replicaPrivateKey = rsaPrivateKey;

  if(config.isReadOnly)
    return true;

  // Load cryptosystem public configurations.
  // Note we reference the Cryptosystems here via unique_ptrs rahter than
  // declaring them by value because we need to declare them here before they
  // are constructed by a helper function, which cannot be done if they are
  // declared by value because they have no default (0-parameter) constructor.
  std::unique_ptr<Cryptosystem> execSys;
  std::unique_ptr<Cryptosystem> slowSys;
  std::unique_ptr<Cryptosystem> commitSys;
  std::unique_ptr<Cryptosystem> optSys;

  if (!deserializeCryptosystemPublicConfiguration(execSys,
                                                  "execution",
                                                  "execution",
                                                  valueAssignments,
                                                  listAssignments,
                                                  filename,
                                                  identifierLines,
                                                  listEntryLines,
                                                  numReplicas)) {
    return false;
  }
  if (!deserializeCryptosystemPublicConfiguration(slowSys,
                                                  "slow path commit",
                                                  "slow_commit",
                                                  valueAssignments,
                                                  listAssignments,
                                                  filename,
                                                  identifierLines,
                                                  listEntryLines,
                                                  numReplicas)) {
    return false;
  }
  if (!deserializeCryptosystemPublicConfiguration(commitSys,
                                                  "commit",
                                                  "commit",
                                                  valueAssignments,
                                                  listAssignments,
                                                  filename,
                                                  identifierLines,
                                                  listEntryLines,
                                                  numReplicas)) {
    return false;
  }
  if (!deserializeCryptosystemPublicConfiguration(optSys,
                                                  "optimistic fast path commit",
                                                  "optimistic_commit",
                                                  valueAssignments,
                                                  listAssignments,
                                                  filename,
                                                  identifierLines,
                                                  listEntryLines,
                                                  numReplicas)) {
    return false;
  }

  // Load private keys for this replica.
  if (!expectEntries({"execution_cryptosystem_private_key",
                      "slow_commit_cryptosystem_private_key",
                      "commit_cryptosystem_private_key",
                      "optimistic_commit_cryptosystem_private_key"},
                     valueAssignments,
                     filename,
                     identifierLines)) {
    return false;
  }

  std::string execPrivateKey = valueAssignments["execution_cryptosystem_private_key"];
  std::string slowPrivateKey = valueAssignments["slow_commit_cryptosystem_private_key"];
  std::string commitPrivateKey = valueAssignments["commit_cryptosystem_private_key"];
  std::string optPrivateKey = valueAssignments["optimistic_commit_cryptosystem_private_key"];

  valueAssignments.erase("execution_cryptosystem_private_key");
  valueAssignments.erase("slow_commit_cryptosystem_private_key");
  valueAssignments.erase("commit_cryptosystem_private_key");
  valueAssignments.erase("optimistic_commit_cryptosystem_private_key");

  if (!execSys->isValidPrivateKey(execPrivateKey)) {
    std::cout << filename << ": line " << identifierLines["execution_cryptosystem_private_key"]
              << ": Invalid private key for selected cryptosystem.\n";
    return false;
  }
  if (!slowSys->isValidPrivateKey(slowPrivateKey)) {
    std::cout << filename << ": line " << identifierLines["slow_commit_cryptosystem_private_key"]
              << ": Invalid private key for selected cryptosystem.\n";
    return false;
  }
  if (!commitSys->isValidPrivateKey(commitPrivateKey)) {
    std::cout << filename << ": line " << identifierLines["commit_cryptosystem_private_key"]
              << ": Invalid private key for selected cryptosystem.\n";
    return false;
  }
  if (!optSys->isValidPrivateKey(optPrivateKey)) {
    std::cout << filename << ": line " << identifierLines["optimistic_commit_cryptosystem_private_key"]
              << ": Invalid private key for selected cryptosystem.\n";
    return false;
  }

  execSys->loadPrivateKey(config.replicaId + 1, execPrivateKey);
  slowSys->loadPrivateKey(config.replicaId + 1, slowPrivateKey);
  commitSys->loadPrivateKey(config.replicaId + 1, commitPrivateKey);
  optSys->loadPrivateKey(config.replicaId + 1, optPrivateKey);

  // Verify that there were not any unexpected parameters specified in the
  // keyfile.
  if ((valueAssignments.size() > 0) || (listAssignments.size() > 0)) {
    for (auto assignment : valueAssignments) {
      std::cout << filename << ": line " << identifierLines[assignment.first]
                << ": Unrecognized parameter: " << assignment.first << ".\n";
    }
    for (auto assignment : listAssignments) {
      std::cout << filename << ": line " << identifierLines[assignment.first]
                << ": Unrecognized parameter: " << assignment.first << ".\n";
    }
    return false;
  }

  // Copy all the information loaded into the replica configuration struct.
  // Note we do not begin copying the information until we know it has been
  // loaded successfully, so the configuration struct will not be left in a
  // partially loaded state.

  config.thresholdSignerForExecution = execSys->createThresholdSigner();
  config.thresholdSignerForSlowPathCommit = slowSys->createThresholdSigner();
  config.thresholdSignerForCommit = commitSys->createThresholdSigner();
  config.thresholdSignerForOptimisticCommit = optSys->createThresholdSigner();

  config.thresholdVerifierForExecution = execSys->createThresholdVerifier();
  config.thresholdVerifierForSlowPathCommit = slowSys->createThresholdVerifier();
  config.thresholdVerifierForCommit = commitSys->createThresholdVerifier();
  config.thresholdVerifierForOptimisticCommit = optSys->createThresholdVerifier();

  return true;
}
