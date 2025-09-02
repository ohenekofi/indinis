// jest.config.js
module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  testMatch: ['**/tssrc/test/**/*.test.ts'], // Ensure this matches your test file location
  testTimeout: 30000, // Increase default timeout for all tests/hooks
  // Add setupFilesAfterEnv if needed for global setup/teardown
};