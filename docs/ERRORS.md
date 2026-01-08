# Errors

All errors produced by CNG are centrally located within two files (one for GRPC and
one for HTTP). These files are gateway/dapiimpl/server_v1/errorhandler.go and
gateway/dataimpl/server_v1/errorhandler.go.

In the interest of making errors easily pareasable by the various clients, CNG
guarentees that it will follow the following assertions (and it is considered
a bug if it does not):

- Error text descriptions must make every effort to guide users to a resolution
  of the issue.
- All details described by the error text description must be represented in the
  error details as well (enabling the reproduction of the text from only the details).
- User data must not exist within an error except as paths described in the
  ResourcePath or Subject fields of an error.
- The Subject field in an error must always be represented as the resource type,
  followed by a slash, followed by the resource path.
