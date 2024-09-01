import base64 from 'base64-js'
interface Secret {
    key: CryptoKey
    iv: Uint8Array
}

interface Cipher {
    encrypt: (data: Uint8Array) => Promise<Uint8Array>
    decrypt: (data: Uint8Array) => Promise<Uint8Array>
}

interface Secrets {
    generateSecret: () => Promise<Secret>
    exportSecret: (secret: Secret) => Promise<JsonWebKey>
    importSecret: (key: JsonWebKey) => Promise<Secret>
}

const cipherFactory = ({
    subtle,
    secret,
}: {
    subtle: SubtleCrypto
    secret: Secret
}): Cipher => {
    const encrypt = async (data: Uint8Array): Promise<Uint8Array> => {
        const encryptedArrayBuffer = await subtle.encrypt(
            {
                name: 'AES-GCM',
                iv: secret.iv,
            },
            secret.key,
            data
        )
        const tag = new Uint8Array(16)
        const encrypted = new Uint8Array(encryptedArrayBuffer)
        encrypted.slice(-16).forEach((byte, index) => {
            tag[index] = byte
        })
        return new Uint8Array([...encrypted.slice(0, -16), ...tag])
    }
    const decrypt = async (data: Uint8Array): Promise<Uint8Array> => {
        const decrypted = await subtle.decrypt(
            {
                name: 'AES-GCM',
                iv: secret.iv,
                tagLength: 128, // 128 bits (16 bytes)
            },
            secret.key,
            data
        )
        return new Uint8Array(decrypted)
    }
    return { encrypt, decrypt }
}

const secretsFactory = ({ subtle }: { subtle: SubtleCrypto }): Secrets => {
    const generateKey = async (): Promise<Secret> => {
        const key = await subtle.generateKey(
            {
                name: 'AES-GCM',
                length: 256,
            },
            true,
            ['encrypt', 'decrypt']
        )
        const iv = new Uint8Array(16)
        for (let i = 0; i < 16; i++) {
            iv[i] = Math.floor(Math.random() * 256)
        }
        return { key, iv }
    }

    const exportKey = async (options: Secret): Promise<JsonWebKey> => {
        const jwk = await subtle.exportKey('jwk', options.key)
        jwk['iv'] = base64.fromByteArray(options.iv)
        return jwk
    }

    const importKey = async (key: JsonWebKey): Promise<Secret> => {
        const importedKey = await subtle.importKey(
            'jwk',
            key,
            { name: 'AES-GCM' },
            true,
            ['encrypt', 'decrypt']
        )
        const iv = base64.toByteArray(key['iv'])
        return { key: importedKey, iv }
    }

    return {
        generateSecret: generateKey,
        exportSecret: exportKey,
        importSecret: importKey,
    }
}

export { Secret, Cipher, cipherFactory, secretsFactory, Secrets }
