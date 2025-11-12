# Branching / Commit Strategy

Feature development should take place in feature branches which ideally take the form
of "$TICKET-ID_$short-desc" (for instance "ING-1111_improved-something"). To maintain
consistency, an underscore should be used to delineate the ticket from the short
description, while dashes should replace spaces in the short description.

Commits are expected to always be 'singular and atomic' such that rolling back the
repository to any commit should be buildable and work as expected. It is acceptable
(and encouraged) to break complicated features into multiple commits which add or
modify specific independent functionality (for instance, DCP support was added
through many commits, initially cleaning up the structure of some areas, then later
refactoring connection management, and finally the inclusion of DCP, each of these
being an independently valuable and working commit).

All development of the next minor or major version of CNG will take place in the
master branch, with the previous minor versions gaining their own branch of the form
vMAJOR.MINOR (v1.1 for example) shortly after release. Additionally, each patch
release receives a tag of the form vMAJOR.MINOR.PATCH (v1.1.10 for example).
