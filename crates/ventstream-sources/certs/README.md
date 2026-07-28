# Provider trust bundles

VentStream packages public CA certificates for database providers that do not
chain to the operating system trust store.

## AWS RDS

- Source: `https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem`
- SHA-256: `e5bb2084ccf45087bda1c9bffdea0eb15ee67f0b91646106e466714f9de3c7e3`
- Certificates: 108
- Retrieved: 2026-07-28

To refresh the bundle, download it from the source above, review AWS's
certificate-rotation guidance, update the checksum in
`crates/ventstream-sources/src/tls.rs`, and run the TLS tests.
