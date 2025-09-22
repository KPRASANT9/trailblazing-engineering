import { createCipheriv, createDecipheriv, randomBytes, scrypt } from 'crypto';
import { promisify } from 'util';

const scryptAsync = promisify(scrypt);

// AES-256-GCM encryption for secure credential storage
export class CredentialCrypto {
  private static algorithm = 'aes-256-gcm';
  private static keyLength = 32;
  private static ivLength = 16;
  private static tagLength = 16;

  private static async deriveKey(password: string, salt: Buffer): Promise<Buffer> {
    return (await scryptAsync(password, salt, this.keyLength)) as Buffer;
  }

  static async encrypt(plaintext: string): Promise<string> {
    if (!process.env.MASTER_KEY) {
      throw new Error('MASTER_KEY environment variable is required for encryption');
    }

    if (!plaintext) {
      return '';
    }

    const salt = randomBytes(16);
    const iv = randomBytes(this.ivLength);
    const key = await this.deriveKey(process.env.MASTER_KEY, salt);
    
    const cipher = createCipheriv(this.algorithm, key, iv) as any;
    
    let encrypted = cipher.update(plaintext, 'utf8', 'hex');
    encrypted += cipher.final('hex');
    
    const authTag = cipher.getAuthTag();
    
    // Combine salt + iv + authTag + encrypted data
    const combined = Buffer.concat([
      salt,
      iv,
      authTag,
      Buffer.from(encrypted, 'hex')
    ]);
    
    return combined.toString('base64');
  }

  static async decrypt(encryptedData: string): Promise<string> {
    if (!process.env.MASTER_KEY) {
      throw new Error('MASTER_KEY environment variable is required for decryption');
    }

    if (!encryptedData) {
      return '';
    }

    try {
      const combined = Buffer.from(encryptedData, 'base64');
      
      // Extract components
      const salt = combined.subarray(0, 16);
      const iv = combined.subarray(16, 16 + this.ivLength);
      const authTag = combined.subarray(16 + this.ivLength, 16 + this.ivLength + this.tagLength);
      const encrypted = combined.subarray(16 + this.ivLength + this.tagLength);
      
      const key = await this.deriveKey(process.env.MASTER_KEY, salt);
      
      const decipher = createDecipheriv(this.algorithm, key, iv) as any;
      decipher.setAuthTag(authTag);
      
      let decrypted = decipher.update(encrypted, undefined, 'utf8');
      decrypted += decipher.final('utf8');
      
      return decrypted;
    } catch (error) {
      console.error('Decryption failed:', error);
      throw new Error('Failed to decrypt credential');
    }
  }
}