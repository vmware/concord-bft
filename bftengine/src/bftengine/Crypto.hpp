//Concord
//
//Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
//This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License. 
//
//This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.

#pragma once

#include <threshsign/ThresholdSignaturesSchemes.h> 

#include <cryptopp/dll.h>
#include <cryptopp/integer.h>

#include <sstream>
#include <string>

using std::string;

namespace bftEngine
{
	namespace impl
	{
		class  CryptographyWrapper
		{
		public:
			static void init(const char* randomSeed);
			static void init();

			static void generateRandomBlock(char *output, size_t size);
		};


		// TODO(GG): define generic signer/verifier (not sure we want to use RSA)

		class RSASigner
		{
		public:
			RSASigner(const char* privteKey, const char* randomSeed);
			RSASigner(const char* privateKey);
			~RSASigner();
			size_t signatureLength();
			bool sign(const char* inBuffer, size_t lengthOfInBuffer, char* outBuffer, size_t lengthOfOutBuffer, size_t& lengthOfReturnedData);
		private:
			void* d;
		};

		class RSAVerifier
		{
		public:
			RSAVerifier(const char* publicKey, const char* randomSeed);
			RSAVerifier(const char* publicKey);
			~RSAVerifier();
			size_t signatureLength();
			bool verify(const char* data, size_t lengthOfData, const char* signature, size_t lengthOfOSignature);
		private:
			void* d;
		};


		class DigestUtil
		{
		public:
			static size_t digestLength();
			static bool compute(const char* input, size_t inputLength, char* outBufferForDigest, size_t lengthOfBufferForDigest);

			class Context
			{
			public:
				Context();
				void update(const char* data, size_t len);
				void writeDigest(char* outDigest); // write digest to outDigest, and invalidate the Context object
				~Context();

			private:
				void* internalState;
			};
		};



		class RSAKeysGenerator
		{
		public:
			static bool generateKeys(std::string& outPublicKey, std::string& outPrivateKey);
			static int getModulusBits();
		};



		class SecretSharingOperations
		{
		public:
			static void splitHexString(uint16_t threshold, uint16_t nShares, string hexData, const char *seed, string* outHexStringArray, uint16_t lenOutHexStringArray);
			static void recoverHexString(uint16_t threshold, string inHexStringArray[], string& outHexData);

			static void splitBinaryString(uint16_t threshold, uint16_t nShares, string binaryData, const char *seed, string* outBinaryStringArray, uint16_t lenOutBinaryStringArray);
			static void recoverBinaryString(uint16_t threshold, string inBinaryStringArray[], string& outBinaryData);
		};

	}
}
