# XDCR Protostellar Behaviour

### PushDocument

PushDocument exposes two values for CAS, one is the CheckCas and one is the StoreCAS. The
CheckCas represents the typical Compare And Swap operation of other memcached commands. If
a CheckCas is specified, conflict resolution is disabled and assuming the CAS check completes,
the write will be accepted. If CheckCas is set to the value of '0' explicitly, it is expected
that the document must not exist and a AddWithMeta will be performed. StoreCAS represents the
new CAS value for the document being pushed, and in cases where conflict resolution is in use,
this is the value that is compared for conflict resolution.

A pseudocode explanation of the logic is:

```
if req.CheckCas != nil:
  if *req.CheckCas == 0:
    AddWithMeta()
  else:
    SetWithMeta(CheckCas: req.CheckCas, Options: SkipConflictResolution)
else:
  SetWithMeta(CheckCas: 0, Options: 0)
```

Additionally, if IsDeleted is specified on the PushDocument operation, it is assumed that this
is an attempt to delete the document, and as such CheckCas will optionally control whether a
pure CAS check is performed (and 0 is an invalid value).

There are a number of conflict related errors that can be returned by the PushDocument operation
based on a number of different factors. Namely whether or not CheckCas is being applied as well as
whether conflict logging is enabled for the operation.

ConflictLogging=true:

- Error:Aborted Code:TRUE_CONFLICT
  This case will occur when the operation would otherwise succeed, but a true conflict was detected
  and the client has not provided an explicit CAS value (to verify they have the existing document
  already available to them locally). This error will also include additional error context which
  includes the conflicted documents meta-data and contents. Note that this error can only be
  returned if the TrueConflicts parameter is set to true.

- Error:Aborted Code:CAS_MISMATCH
  This case will occur when a CAS was explicitly provided to the operation, but the target document
  has been updated and has a different CAS value.

- Error:Aborted Code:DOC_NEWER
  This case occurs when a new document is rejected because it is older than the document that
  already exists. The concept of 'newer' uses the buckets configured conflict resolution mode.

ConflictLogging=false:

### CheckDocument

CheckDocument has precisely the same mechanics as PushDocument does, with the exception that in the
cases where a PushDocument operation would write changes to the document, CheckDocument will return
success and do no write.
