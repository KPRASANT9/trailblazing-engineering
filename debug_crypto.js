// Simple debug script to test encryption
const { CredentialCrypto } = require('./dist/server/crypto.js');

async function testEncryption() {
  try {
    console.log('Testing encryption...');
    const plaintext = 'password123';
    console.log('Plaintext:', plaintext);
    
    const encrypted = await CredentialCrypto.encrypt(plaintext);
    console.log('Encrypted:', encrypted);
    
    const decrypted = await CredentialCrypto.decrypt(encrypted);
    console.log('Decrypted:', decrypted);
    
    console.log('Encryption test successful!');
  } catch (error) {
    console.error('Encryption test failed:', error);
  }
}

testEncryption();