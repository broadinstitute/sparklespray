import certifi

with open("go/src/github.com/broadinstitute/sparklesworker/certs.go", "wt") as o:
    with open(certifi.where()) as fd:
        o.write(
            """
// autogenerated from bundle_certs.py
package kubequeconsume

const pemCerts = `"""
        )
        o.write(fd.read())
        o.write("`\n")
