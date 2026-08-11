# Haste

Sharing code is a good thing, and it should be _really_ easy to do it. A lot
of times, you want to show someone something you're seeing — and that's what
pastebins are for.

## Basic Usage

Type what you want someone to see, click "Save", and copy the URL. Send that
URL to someone and they'll see what you see.

To start a new paste, click "New" (or press `Alt` + `N`).

## Keyboard shortcuts

| Keys | Action |
|---|---|
| `Ctrl` + `S` | Save |
| `Alt` + `N` | New paste |
| `Ctrl` + `E` | Edit (creates a new paste — the original isn't overwritten) |
| `Alt` + `R` | View raw |
| `Ctrl` + `Shift` + `S` | Share / copy link |

## From the Console

The original `/documents` and `/raw` API is still here, so anything that
worked against haste-server before still works against this instance:

```
cat something | curl -s -X POST --data-binary @- https://this-host/documents
```

or, with the classic `haste-client`, `cat something | haste`.

## Deleting a paste

Pastes made anonymously can be deleted by whoever created them, from the
browser they created it in — a "Delete" button appears on the paste's page.
There are no accounts; that revocation right is the only ownership this
server tracks.

## Privacy

Post things at your own risk — there's no expectation of privacy, and pastes
may be removed by the operator at any time and without notice.

## About this fork

This instance runs [gradippp/haste-server](https://github.com/gradippp/haste-server),
a fork and rewrite of the original
[haste-server](https://github.com/toptal/haste-server) by John Crepezzi. It
replaces the original's plain-JavaScript server and static frontend with a
single Next.js/TypeScript application — new editor and viewer UI, all storage
backends and key generators ported to TypeScript, a test suite, and a
modernized Docker setup — while keeping the original `/documents` and `/raw`
API compatible, so existing clients and scripts keep working unmodified. See
the repository's README for the full list of what changed.

## Original project

* [haste-client](https://github.com/seejohnrun/haste-client)
* [haste-server](https://github.com/toptal/haste-server)

Original code by John Crepezzi <john.crepezzi@gmail.com>.
Original key design by Brian Dawson <bridawson@gmail.com>.
