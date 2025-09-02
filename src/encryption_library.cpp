// @src/encryption_library.cpp

#include "../../include/encryption_library.h"
#include "../../include/debug_utils.h"
#include <algorithm>
#include <cstring>

// Platform-specific includes and definitions
#ifdef _WIN32
    #define WIN32_LEAN_AND_MEAN
    #include <windows.h>
    #include <bcrypt.h>
    #pragma comment(lib, "bcrypt.lib") // Link against the required system library

    // Define STATUS_AUTH_TAG_MISMATCH if not defined by the SDK
    #ifndef STATUS_AUTH_TAG_MISMATCH
    #define STATUS_AUTH_TAG_MISMATCH ((NTSTATUS)0xC000A002)
    #endif

    // Helper macro for Windows CNG error checking
    #define CHECK_NTSTATUS(status, operation) \
        if (!BCRYPT_SUCCESS(status)) { \
            char hexbuf[16]; \
            sprintf_s(hexbuf, sizeof(hexbuf), "%08lX", static_cast<unsigned long>(status)); \
            throw CryptoException(std::string(operation) + " failed with NTSTATUS: 0x" + hexbuf); \
        }
        
#elif __APPLE__
    #include <CommonCrypto/CommonCrypto.h>
    #include <CommonCrypto/CommonRandom.h>
    #include <CommonCrypto/CommonKeyDerivation.h>
    
    // Helper macro for macOS CommonCrypto error checking
    #define CHECK_CC_STATUS(status, operation) \
        if (status != kCCSuccess) { \
            throw CryptoException(std::string(operation) + " failed with status: " + std::to_string(status)); \
        }
        
#else // Linux and other Unix-like systems
    #include <openssl/evp.h>
    #include <openssl/rand.h>
    #include <openssl/err.h>
    #include <openssl/kdf.h>
    
    // Helper function for OpenSSL error reporting
    static std::string getOpenSSLError() {
        unsigned long err_code = ERR_get_error();
        if (err_code == 0) return "No error";
        char buffer[256];
        ERR_error_string_n(err_code, buffer, sizeof(buffer));
        return std::string(buffer);
    }
    
    #define CHECK_OPENSSL_RESULT(result, operation) \
        if (result != 1) { \
            throw CryptoException(std::string(operation) + " failed: " + getOpenSSLError()); \
        }
        
    // RAII wrapper for OpenSSL EVP_CIPHER_CTX
    class EVPCipherCtx {
    public:
        EVPCipherCtx() : ctx(EVP_CIPHER_CTX_new()) {
            if (!ctx) throw CryptoException("Failed to create EVP cipher context");
        }
        ~EVPCipherCtx() { 
            if (ctx) EVP_CIPHER_CTX_free(ctx); 
        }
        EVP_CIPHER_CTX* get() const { return ctx; }
    private:
        EVP_CIPHER_CTX* ctx;
        EVPCipherCtx(const EVPCipherCtx&) = delete;
        EVPCipherCtx& operator=(const EVPCipherCtx&) = delete;
    };
#endif

// Define the static constant
const std::string EncryptionLibrary::KEK_VALIDATION_PLAINTEXT = "IndinisDB_KEK_Validation_OK_Value_v1.0";

// Exception constructor
CryptoException::CryptoException(const std::string& message)
    : std::runtime_error("Crypto Error: " + message) {}

// --- Random Number Generation (Unchanged, but verified correct) ---
std::vector<unsigned char> EncryptionLibrary::generateRandomBytes(int size) {
    if (size <= 0) { throw CryptoException("Invalid size for random bytes generation"); }
    std::vector<unsigned char> bytes(size);
#ifdef _WIN32
    CHECK_NTSTATUS(BCryptGenRandom(NULL, bytes.data(), size, BCRYPT_USE_SYSTEM_PREFERRED_RNG), "BCryptGenRandom");
#elif __APPLE__
    CHECK_CC_STATUS(CCRandomGenerateBytes(bytes.data(), size), "CCRandomGenerateBytes");
#else
    CHECK_OPENSSL_RESULT(RAND_bytes(bytes.data(), size), "RAND_bytes");
#endif
    return bytes;
}

// --- Key Derivation (Unchanged, but verified correct) ---
std::vector<unsigned char> EncryptionLibrary::deriveKeyFromPassword(
    const std::string& password, const std::vector<unsigned char>& salt, int iterations) {
    if (password.empty()) { throw CryptoException("Password cannot be empty"); }
    if (salt.size() != SALT_SIZE) { throw CryptoException("Invalid salt size"); }
    int effective_iterations = (iterations > 0) ? iterations : DEFAULT_PBKDF2_ITERATIONS;
    std::vector<unsigned char> key(AES_KEY_SIZE);
#ifdef _WIN32
    BCRYPT_ALG_HANDLE hAlg = NULL;
    CHECK_NTSTATUS(BCryptOpenAlgorithmProvider(&hAlg, BCRYPT_SHA256_ALGORITHM, NULL, BCRYPT_ALG_HANDLE_HMAC_FLAG), "BCryptOpenAlgorithmProvider (PBKDF2)");
    CHECK_NTSTATUS(BCryptDeriveKeyPBKDF2(hAlg, (PUCHAR)password.c_str(), (ULONG)password.length(), (PUCHAR)salt.data(), (ULONG)salt.size(), effective_iterations, key.data(), (ULONG)key.size(), 0), "BCryptDeriveKeyPBKDF2");
    BCryptCloseAlgorithmProvider(hAlg, 0);
#elif __APPLE__
    CHECK_CC_STATUS(CCKeyDerivationPBKDF(kCCPBKDF2, password.c_str(), password.length(), salt.data(), salt.size(), kCCPRFHmacAlgSHA256, effective_iterations, key.data(), key.size()), "CCKeyDerivationPBKDF");
#else
    CHECK_OPENSSL_RESULT(PKCS5_PBKDF2_HMAC(password.c_str(), password.length(), salt.data(), salt.size(), effective_iterations, EVP_sha256(), key.size(), key.data()), "PKCS5_PBKDF2_HMAC");
#endif
    return key;
}

// --- Internal AES-256-GCM Implementation ---

EncryptionLibrary::EncryptedData EncryptionLibrary::encryptAES_GCM_Internal(
    const unsigned char* plaintext, size_t plaintext_len,
    const std::vector<unsigned char>& key,
    const std::vector<unsigned char>& iv_nonce,
    const std::vector<unsigned char>& aad_data) {
    
    if (key.size() != AES_KEY_SIZE) throw CryptoException("Invalid key size for AES-256");
    if (iv_nonce.size() != AES_GCM_IV_SIZE) throw CryptoException("Invalid IV size for AES-GCM");
    
    EncryptedData result;
    result.iv = iv_nonce;
    result.data.resize(plaintext_len); // Ciphertext is same size as plaintext in GCM
    result.tag.resize(AES_GCM_TAG_SIZE);

#ifdef _WIN32
    BCRYPT_ALG_HANDLE hAlg = NULL; BCRYPT_KEY_HANDLE hKey = NULL;
    try {
        CHECK_NTSTATUS(BCryptOpenAlgorithmProvider(&hAlg, BCRYPT_AES_ALGORITHM, NULL, 0), "BCryptOpenAlgorithmProvider (GCM)");
        CHECK_NTSTATUS(BCryptSetProperty(hAlg, BCRYPT_CHAINING_MODE, (PUCHAR)BCRYPT_CHAIN_MODE_GCM, sizeof(BCRYPT_CHAIN_MODE_GCM), 0), "BCryptSetProperty (GCM Mode)");
        CHECK_NTSTATUS(BCryptGenerateSymmetricKey(hAlg, &hKey, NULL, 0, (PUCHAR)key.data(), key.size(), 0), "BCryptGenerateSymmetricKey");
        BCRYPT_AUTHENTICATED_CIPHER_MODE_INFO authInfo;
        BCRYPT_INIT_AUTH_MODE_INFO(authInfo);
        authInfo.pbNonce = (PUCHAR)iv_nonce.data(); authInfo.cbNonce = iv_nonce.size();
        authInfo.pbTag = result.tag.data(); authInfo.cbTag = result.tag.size();
        if (!aad_data.empty()) {
            authInfo.pbAuthData = (PUCHAR)aad_data.data(); authInfo.cbAuthData = aad_data.size();
        }
        DWORD bytesWritten;
        CHECK_NTSTATUS(BCryptEncrypt(hKey, (PUCHAR)plaintext, plaintext_len, &authInfo, NULL, 0, result.data.data(), result.data.size(), &bytesWritten, 0), "BCryptEncrypt");
        if (bytesWritten != plaintext_len) throw CryptoException("BCryptEncrypt wrote an unexpected number of bytes.");
    } catch (...) {
        if (hKey) BCryptDestroyKey(hKey);
        if (hAlg) BCryptCloseAlgorithmProvider(hAlg, 0);
        throw;
    }
    if (hKey) BCryptDestroyKey(hKey);
    if (hAlg) BCryptCloseAlgorithmProvider(hAlg, 0);
    
#elif __APPLE__
    // --- CORRECTED macOS IMPLEMENTATION ---
    CCCryptorRef cryptor;
    CHECK_CC_STATUS(CCCryptorCreate(kCCEncrypt, kCCAlgorithmAES, kCCOptionECBMode, key.data(), key.size(), NULL, &cryptor), "CCCryptorCreate (GCM)");
    CHECK_CC_STATUS(CCCryptorGCMAddIV(cryptor, iv_nonce.data(), iv_nonce.size()), "CCCryptorGCMAddIV");
    if (!aad_data.empty()) {
        CHECK_CC_STATUS(CCCryptorGCMAddAAD(cryptor, aad_data.data(), aad_data.size()), "CCCryptorGCMAddAAD");
    }
    size_t written_len = 0;
    CHECK_CC_STATUS(CCCryptorUpdate(cryptor, plaintext, plaintext_len, result.data.data(), result.data.size(), &written_len), "CCCryptorUpdate (GCM)");
    size_t final_len = 0;
    CHECK_CC_STATUS(CCCryptorFinal(cryptor, result.data.data() + written_len, result.data.size() - written_len, &final_len), "CCCryptorFinal (GCM)");
    written_len += final_len;
    if (written_len != plaintext_len) throw CryptoException("macOS GCM encryption wrote unexpected number of bytes.");
    CHECK_CC_STATUS(CCCryptorGCMGetTag(cryptor, result.tag.data(), &written_len), "CCCryptorGCMGetTag");
    if (written_len != AES_GCM_TAG_SIZE) throw CryptoException("macOS GCM encryption produced incorrect tag size.");
    CCCryptorRelease(cryptor);

#else // Linux - OpenSSL
    EVPCipherCtx ctx; int len = 0;
    CHECK_OPENSSL_RESULT(EVP_EncryptInit_ex(ctx.get(), EVP_aes_256_gcm(), NULL, key.data(), iv_nonce.data()), "EVP_EncryptInit_ex (GCM)");
    if (!aad_data.empty()) {
        CHECK_OPENSSL_RESULT(EVP_EncryptUpdate(ctx.get(), NULL, &len, aad_data.data(), aad_data.size()), "EVP_EncryptUpdate (AAD)");
    }
    CHECK_OPENSSL_RESULT(EVP_EncryptUpdate(ctx.get(), result.data.data(), &len, plaintext, plaintext_len), "EVP_EncryptUpdate");
    int ciphertext_len = len;
    CHECK_OPENSSL_RESULT(EVP_EncryptFinal_ex(ctx.get(), result.data.data() + len, &len), "EVP_EncryptFinal_ex");
    ciphertext_len += len;
    if (static_cast<size_t>(ciphertext_len) != plaintext_len) throw CryptoException("OpenSSL GCM encryption wrote unexpected number of bytes.");
    CHECK_OPENSSL_RESULT(EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_GCM_GET_TAG, result.tag.size(), result.tag.data()), "EVP_CTRL_GCM_GET_TAG");
#endif

    return result;
}

std::vector<unsigned char> EncryptionLibrary::decryptAES_GCM_Internal(
    const EncryptedData& encData,
    const std::vector<unsigned char>& key,
    const std::vector<unsigned char>& aad_data) {
    
    if (key.size() != AES_KEY_SIZE) throw CryptoException("Invalid key size for AES-256");
    if (encData.iv.size() != AES_GCM_IV_SIZE) throw CryptoException("Invalid IV size for AES-GCM");
    if (encData.tag.size() != AES_GCM_TAG_SIZE) throw CryptoException("Invalid tag size for AES-GCM");
    
    std::vector<unsigned char> plaintext(encData.data.size());

#ifdef _WIN32
    BCRYPT_ALG_HANDLE hAlg = NULL; BCRYPT_KEY_HANDLE hKey = NULL;
    try {
        CHECK_NTSTATUS(BCryptOpenAlgorithmProvider(&hAlg, BCRYPT_AES_ALGORITHM, NULL, 0), "BCryptOpenAlgorithmProvider (GCM Decrypt)");
        CHECK_NTSTATUS(BCryptSetProperty(hAlg, BCRYPT_CHAINING_MODE, (PUCHAR)BCRYPT_CHAIN_MODE_GCM, sizeof(BCRYPT_CHAIN_MODE_GCM), 0), "BCryptSetProperty (GCM Mode Decrypt)");
        CHECK_NTSTATUS(BCryptGenerateSymmetricKey(hAlg, &hKey, NULL, 0, (PUCHAR)key.data(), key.size(), 0), "BCryptGenerateSymmetricKey (Decrypt)");
        BCRYPT_AUTHENTICATED_CIPHER_MODE_INFO authInfo;
        BCRYPT_INIT_AUTH_MODE_INFO(authInfo);
        authInfo.pbNonce = (PUCHAR)encData.iv.data(); authInfo.cbNonce = encData.iv.size();
        authInfo.pbTag = (PUCHAR)encData.tag.data(); authInfo.cbTag = encData.tag.size();
        if (!aad_data.empty()) {
            authInfo.pbAuthData = (PUCHAR)aad_data.data(); authInfo.cbAuthData = aad_data.size();
        }
        DWORD bytesWritten;
        // For BCryptDecrypt, the tag check failure returns a specific status code
        NTSTATUS status = BCryptDecrypt(hKey, (PUCHAR)encData.data.data(), encData.data.size(), &authInfo, NULL, 0, plaintext.data(), plaintext.size(), &bytesWritten, 0);
        if (status == STATUS_AUTH_TAG_MISMATCH) { throw CryptoException("BCryptDecrypt: Authentication tag mismatch."); }
        CHECK_NTSTATUS(status, "BCryptDecrypt");
        plaintext.resize(bytesWritten);
    } catch (...) {
        if (hKey) BCryptDestroyKey(hKey);
        if (hAlg) BCryptCloseAlgorithmProvider(hAlg, 0);
        throw;
    }
    if (hKey) BCryptDestroyKey(hKey);
    if (hAlg) BCryptCloseAlgorithmProvider(hAlg, 0);

#elif __APPLE__
    // --- CORRECTED macOS IMPLEMENTATION ---
    CCCryptorRef cryptor;
    CHECK_CC_STATUS(CCCryptorCreate(kCCDecrypt, kCCAlgorithmAES, kCCOptionECBMode, key.data(), key.size(), NULL, &cryptor), "CCCryptorCreate (GCM Decrypt)");
    CHECK_CC_STATUS(CCCryptorGCMAddIV(cryptor, encData.iv.data(), encData.iv.size()), "CCCryptorGCMAddIV (Decrypt)");
    if (!aad_data.empty()) {
        CHECK_CC_STATUS(CCCryptorGCMAddAAD(cryptor, aad_data.data(), aad_data.size()), "CCCryptorGCMAddAAD (Decrypt)");
    }
    size_t written_len = 0;
    CHECK_CC_STATUS(CCCryptorUpdate(cryptor, encData.data.data(), encData.data.size(), plaintext.data(), plaintext.size(), &written_len), "CCCryptorUpdate (GCM Decrypt)");
    // Set the tag *before* calling final
    CHECK_CC_STATUS(CCCryptorGCMSetTag(cryptor, encData.tag.data(), encData.tag.size()), "CCCryptorGCMSetTag");
    size_t final_len = 0;
    CCCryptorStatus final_status = CCCryptorFinal(cryptor, plaintext.data() + written_len, plaintext.size() - written_len, &final_len);
    if (final_status == kCCAuthError) {
        CCCryptorRelease(cryptor);
        throw CryptoException("CommonCrypto GCM decryption failed: Authentication tag mismatch.");
    }
    CHECK_CC_STATUS(final_status, "CCCryptorFinal (GCM Decrypt)");
    written_len += final_len;
    plaintext.resize(written_len);
    CCCryptorRelease(cryptor);

#else // Linux - OpenSSL
    EVPCipherCtx ctx; int len = 0; int plaintext_len = 0;
    CHECK_OPENSSL_RESULT(EVP_DecryptInit_ex(ctx.get(), EVP_aes_256_gcm(), NULL, key.data(), encData.iv.data()), "EVP_DecryptInit_ex (GCM)");
    if (!aad_data.empty()) {
        CHECK_OPENSSL_RESULT(EVP_DecryptUpdate(ctx.get(), NULL, &len, aad_data.data(), aad_data.size()), "EVP_DecryptUpdate (AAD)");
    }
    CHECK_OPENSSL_RESULT(EVP_DecryptUpdate(ctx.get(), plaintext.data(), &len, encData.data.data(), encData.data.size()), "EVP_DecryptUpdate");
    plaintext_len = len;
    CHECK_OPENSSL_RESULT(EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_GCM_SET_TAG, encData.tag.size(), (void*)encData.tag.data()), "EVP_CTRL_GCM_SET_TAG");
    if (EVP_DecryptFinal_ex(ctx.get(), plaintext.data() + len, &len) != 1) { // Returns 0 on tag mismatch, not just error
        throw CryptoException("AES-GCM decryption failed: Authentication tag mismatch.");
    }
    plaintext_len += len;
    plaintext.resize(plaintext_len);
#endif

    return plaintext;
}

// --- Public API Implementation ---

EncryptionLibrary::EncryptedData EncryptionLibrary::encryptWithAES_GCM(
    const std::string& plaintext, const std::vector<unsigned char>& key,
    const std::vector<unsigned char>& iv_nonce, const std::vector<unsigned char>& aad_data) {
    return encryptAES_GCM_Internal(reinterpret_cast<const unsigned char*>(plaintext.c_str()), plaintext.length(), key, iv_nonce, aad_data);
}

// THIS IS THE NEW OVERLOAD
EncryptionLibrary::EncryptedData EncryptionLibrary::encryptWithAES_GCM(
    const std::vector<unsigned char>& plaintext, const std::vector<unsigned char>& key,
    const std::vector<unsigned char>& iv_nonce, const std::vector<unsigned char>& aad_data) {
    return encryptAES_GCM_Internal(plaintext.data(), plaintext.size(), key, iv_nonce, aad_data);
}

std::string EncryptionLibrary::decryptWithAES_GCM(
    const EncryptedData& encData, const std::vector<unsigned char>& key, const std::vector<unsigned char>& aad_data) {
    std::vector<unsigned char> plaintext = decryptAES_GCM_Internal(encData, key, aad_data);
    return std::string(plaintext.begin(), plaintext.end());
}

std::vector<unsigned char> EncryptionLibrary::decryptWithAES_GCM_Binary(
    const EncryptedData& encData, const std::vector<unsigned char>& key, const std::vector<unsigned char>& aad_data) {
    return decryptAES_GCM_Internal(encData, key, aad_data);
}

// --- Key Wrapping (Unchanged, already correct) ---
EncryptionLibrary::EncryptedData EncryptionLibrary::wrapKeyAES_GCM(
    const std::vector<unsigned char>& key_to_wrap, const std::vector<unsigned char>& wrapping_key,
    const std::vector<unsigned char>& iv_nonce) {
    return encryptAES_GCM_Internal(key_to_wrap.data(), key_to_wrap.size(), wrapping_key, iv_nonce, {});
}

std::vector<unsigned char> EncryptionLibrary::unwrapKeyAES_GCM(
    const EncryptedData& wrapped_key_data, const std::vector<unsigned char>& unwrapping_key) {
    return decryptAES_GCM_Internal(wrapped_key_data, unwrapping_key, {});
}

// --- KEK Validation (Unchanged, already correct) ---
EncryptionLibrary::EncryptedData EncryptionLibrary::encryptKekValidationValue(
    const std::string& plaintext, const std::vector<unsigned char>& kek, const std::vector<unsigned char>& iv_nonce) {
    return encryptWithAES_GCM(plaintext, kek, iv_nonce, {});
}

bool EncryptionLibrary::verifyKekValidationValue(
    const EncryptedData& encrypted_validation_data, const std::vector<unsigned char>& kek,
    const std::string& expected_plaintext) {
    try {
        return decryptWithAES_GCM(encrypted_validation_data, kek, {}) == expected_plaintext;
    } catch (const CryptoException&) {
        return false;
    }
}