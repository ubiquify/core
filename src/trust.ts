import { Link } from './types'

interface Signer {
    sign: (root: Link) => Promise<Uint8Array>
    exportPublicKey: () => Promise<JsonWebKey>
    name?: string
    email?: string
}

const signerFactory = ({
    name,
    email,
    subtle,
    privateKey,
    publicKey,
}: {
    name?: string
    email?: string
    subtle: SubtleCrypto
    privateKey: CryptoKey
    publicKey: CryptoKey
}): Signer => {
    const sign = async (root: Link): Promise<Uint8Array> => {
        const buffer: ArrayBuffer = await subtle.sign(
            {
                name: 'RSA-PSS',
                saltLength: 32,
            },
            privateKey,
            root.bytes
        )
        return new Uint8Array(buffer)
    }

    const exportPublicKey = async (): Promise<JsonWebKey> => {
        return await subtle.exportKey('jwk', publicKey)
    }
    return { name, email, sign, exportPublicKey }
}

const verify = async ({
    subtle,
    publicKey,
    root,
    signature,
}: {
    subtle: SubtleCrypto
    publicKey: CryptoKey
    root: Link
    signature: Uint8Array
}): Promise<boolean> => {
    return await subtle.verify(
        {
            name: 'RSA-PSS',
            saltLength: 32,
        },
        publicKey,
        signature,
        root.bytes
    )
}

const importPublicKey = async ({
    subtle,
    key,
}: {
    subtle: SubtleCrypto
    key: JsonWebKey
}): Promise<CryptoKey> => {
    return await subtle.importKey(
        'jwk',
        key,
        { name: 'RSA-PSS', hash: 'SHA-256' },
        true,
        ['verify']
    )
}

export { signerFactory, Signer, verify, importPublicKey }
