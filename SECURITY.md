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
trust. The database password is read by the database driver (for PostgreSQL, a
`.pgpass` file or `PGPASSWORD`), and not from the `.env` file. The options
`--password` and `--api-key` of the `extract` commands take the secret on the
command line, where it stays in the shell history, so use an account that can
only read what you extract, and clear the history afterwards.
