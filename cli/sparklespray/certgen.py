from OpenSSL import crypto, SSL
from socket import gethostname
from time import gmtime, mktime
from typing import Tuple


def create_self_signed_cert() -> Tuple[bytes, bytes]:
    # create a key pair
    k = crypto.PKey()
    k.generate_key(crypto.TYPE_RSA, 2048)

    # create a self-signed cert, valid for 10 years
    cert = crypto.X509()
    cert.get_subject().O = "Acme Co"
    cert.get_subject().CN = "sparkles.server"
    cert.set_serial_number(1000)
    cert.gmtime_adj_notBefore(0)
    cert.gmtime_adj_notAfter(10 * 365 * 24 * 60 * 60)
    cert.set_issuer(cert.get_subject())
    cert.set_pubkey(k)
    cert.sign(k, "sha256")

    cert_bytes = crypto.dump_certificate(crypto.FILETYPE_PEM, cert)
    key_bytes = crypto.dump_privatekey(crypto.FILETYPE_PEM, k)

    return cert_bytes, key_bytes
