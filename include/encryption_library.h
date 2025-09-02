// @include/encryption_library.h
#pragma once

#include "types.h" // For EncryptionScheme and constants
#include <string>
#include <vector>
#include <stdexcept>

// Cross-platform encryption library using native OS cryptographic APIs
// Windows: CNG (Cryptography Next Generation)
// macOS: CommonCrypto
// Linux: OpenSSL (as fallback for compatibility)
class CryptoException : public std::runtime_error {
public:
    explicit CryptoException(const std::string& message);
};

class EncryptionLibrary {
public:
    // Algorithm constants
    static constexpr int AES_KEY_SIZE = 32;
    static constexpr int AES_GCM_IV_SIZE = 12;
    static constexpr int AES_GCM_TAG_SIZE = 16;
    static constexpr int SALT_SIZE = 16;
    static constexpr int DEFAULT_PBKDF2_ITERATIONS = 600000;
    
    static const std::string KEK_VALIDATION_PLAINTEXT;

    struct EncryptedData {
        std::vector<unsigned char> data;
        std::vector<unsigned char> iv;
        std::vector<unsigned char> tag;
        
        EncryptedData() = default;
        EncryptedData(const std::vector<unsigned char>& ciphertext,
                     const std::vector<unsigned char>& init_vector,
                     const std::vector<unsigned char>& auth_tag)
            : data(ciphertext), iv(init_vector), tag(auth_tag) {}
    };

    // --- Core Primitives ---
    static std::vector<unsigned char> generateRandomBytes(int size);
    static std::vector<unsigned char> deriveKeyFromPassword(
        const std::string& password,
        const std::vector<unsigned char>& salt,
        int iterations = DEFAULT_PBKDF2_ITERATIONS);

    // --- AES-256-GCM ---
    static EncryptedData encryptWithAES_GCM(
        const std::string& plaintext,
        const std::vector<unsigned char>& key,
        const std::vector<unsigned char>& iv_nonce,
        const std::vector<unsigned char>& aad_data = {});

    static EncryptedData encryptWithAES_GCM(
        const std::vector<unsigned char>& plaintext,
        const std::vector<unsigned char>& key,
        const std::vector<unsigned char>& iv_nonce,
        const std::vector<unsigned char>& aad_data = {});

    static std::string decryptWithAES_GCM(
        const EncryptedData& encData,
        const std::vector<unsigned char>& key,
        const std::vector<unsigned char>& aad_data = {});

    static std::vector<unsigned char> decryptWithAES_GCM_Binary(
        const EncryptedData& encData,
        const std::vector<unsigned char>& key,
        const std::vector<unsigned char>& aad_data = {});

    // --- Key Wrapping ---
    static EncryptedData wrapKeyAES_GCM(
        const std::vector<unsigned char>& key_to_wrap,
        const std::vector<unsigned char>& wrapping_key,
        const std::vector<unsigned char>& iv_nonce);

    static std::vector<unsigned char> unwrapKeyAES_GCM(
        const EncryptedData& wrapped_key_data,
        const std::vector<unsigned char>& unwrapping_key);

    // --- KEK Validation ---
    static EncryptedData encryptKekValidationValue(
        const std::string& plaintext,
        const std::vector<unsigned char>& kek,
        const std::vector<unsigned char>& iv_nonce);
        
    static bool verifyKekValidationValue(
        const EncryptedData& encrypted_validation_data,
        const std::vector<unsigned char>& kek,
        const std::string& expected_plaintext);

    // --- Utilities ---
    static std::vector<unsigned char> generateIV() {
        return generateRandomBytes(AES_GCM_IV_SIZE);
    }
    
    static std::vector<unsigned char> generateSalt() {
        return generateRandomBytes(SALT_SIZE);
    }
    
    static std::vector<unsigned char> generateAESKey() {
        return generateRandomBytes(AES_KEY_SIZE);
    }

private:
    // Internal platform-specific implementation functions
    static EncryptedData encryptAES_GCM_Internal(
        const unsigned char* plaintext, size_t plaintext_len,
        const std::vector<unsigned char>& key,
        const std::vector<unsigned char>& iv_nonce,
        const std::vector<unsigned char>& aad_data);
        
    static std::vector<unsigned char> decryptAES_GCM_Internal(
        const EncryptedData& encData,
        const std::vector<unsigned char>& key,
        const std::vector<unsigned char>& aad_data);
};