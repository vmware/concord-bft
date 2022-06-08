set(eddsa_openssl_source_files
        EdDSAMultisigFactory.cpp
        SSLEdDSAPublicKey.cpp
        SSLEdDSAPrivateKey.cpp
        EdDSAMultisigSigner.cpp
        EdDSAMultisigVerifier.cpp)

add_library(eddsa_openssl STATIC ${eddsa_openssl_source_files})
target_include_directories(eddsa_openssl PRIVATE ../../include)