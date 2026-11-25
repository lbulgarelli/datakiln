# Security

## Reporting a vulnerability

Please do not open a public issue for a security problem. Write to
lucas.bulgarelli@hotmail.com with what you found, how to reproduce it, and the
version of datakiln. I will answer, and fix the problem in a
new release, with credit to you if you want it.

## Supported versions

Only the latest release is supported.

## What to keep in mind when you use it

datakiln runs the SQL files of a dataset against a database and downloads
data with the credentials it is given, so run only the datasets that you
trust. The passwords and the REDCap token are read from the `.build.env` and
`.extract.env` files of the dataset, so keep those files out of Git and
readable only by you, and use an account for the source that can only read what
you extract.
