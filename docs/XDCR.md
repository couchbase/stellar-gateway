# XDCR Protostellar Behaviour

### PushDocument

PushDocument exposes two values for CAS, one is the CheckCAS and one is the StoreCAS. The
CheckCAS represents the typical Compare And Swap operation of other memcached commands. If
a CheckCAS is specified, conflict resolution is disabled and assuming the CAS check completes,
the write will be accepted. If CheckCAS is set to the value of '0' explicitly, it is expected
that the document must not exist and a AddWithMeta will be performed. StoreCAS represents the
new CAS value for the document being pushed, and in cases where conflict resolution is in use,
this is the value that is compared for conflict resolution.

A pseudocode explanation of the logic is:

```
if req.CheckCAS != nil:
  if *req.CheckCAS == 0:
    AddWithMeta()
  else:
    SetWithMeta(CheckCAS: req.CheckCas, Options: SkipConflictResolution)
else:
  SetWithMeta(CheckCAS: 0, Options: 0)
```

### DeleteDocument

DeleteDocument follows the same rules as PushDocument, with the exception that a CheckCAS value
of 0 is an inherently invalid value (it does not make sense to explicitly check CAS against a
value that is not valid).
