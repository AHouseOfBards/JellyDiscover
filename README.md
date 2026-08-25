# JellyDiscover

Personalised recommendation libraries for every user on your Jellyfin server.

JellyDiscover 2.0 is a **complete rewrite** — now a native Jellyfin plugin instead of a
standalone Python app. It replaces the deprecated 1.x engine entirely.

Each user gets their own **Discover Movies / Discover Shows / Discover Music** library,
visible only to them, refreshed as they watch. Because a library is a server-side object,
it renders on every client — web, Android TV, Roku, Swiftfin, Kodi — with no client-side
support required.

> **Pre-release.** The recommendation core is covered by 50 tests that run in under a
> second. The Jellyfin integration layer compiles against 10.11.5 but is still being
> validated against live servers. Back up your Jellyfin config before installing, or test
> on a non-production instance.

---

## Why a rewrite?

JellyDiscover 1.x was a standalone Python app that drove Jellyfin over HTTP. It worked, but
had fundamental architectural problems — it managed Jellyfin state without keeping any
record of what it had created, re-deriving its own identity on every run by
substring-matching library names and file paths.

Running as a native plugin eliminates the entire delivery layer — the 229 MB installer,
Windows service, Flask dashboard on port 5000, the scheduler, and path substitution.

| | 1.x (Python) | 2.0 (plugin) |
|---|---|---|
| Install | 229 MB installer, admin rights, Windows password | Paste a repo URL, click install |
| Payload | 3 bundled executables | 2 DLLs, 174 KB |
| Config UI | Flask on `:5000`, **no authentication** | Jellyfin dashboard, admin auth inherited |
| Scheduling | thread comparing `HH:MM` strings | Jellyfin's own task scheduler |
| Item ceiling | first **600** items, unsorted, unpaged | the entire catalogue |
| Path substitution | required, and inverted (broke playback) | cannot exist — same process, same filesystem |
| Identity | library **list index**, mangled display name | a stored registry, keyed on user id |
| Uninstall | *"you must delete the libraries manually"* | one button, plus a best-effort hook |

---

## How the recommendations work

Five stages, all in `JellyDiscover.Core`, which has **zero dependencies** — no Jellyfin,
no I/O, no clock. That is what makes the whole algorithm testable in milliseconds.

**1 · Signals.** Labels come from data 1.x downloaded on every run and never read:

| Label | From |
|---|---|
| Strong positive | `PlayCount >= 2` (a rewatch), favourited, rated ≥ 8 |
| Positive | finished (> 90%) |
| **Strong negative** | **started, under 20%, not resumed in 30 days** |
| Weak negative | in the library 90+ days, never started |

Abandonment is the only real negative signal a media server produces, and it is the only
thing that can teach the model what to *stop* suggesting.

**2 · Representation.** Each item becomes a BM25 term vector built from name, genres, tags,
cast, director, studio and overview — not the plot summary alone, which is the weakest text
on an item. **Inverse document frequency is learned from your actual library**, so there is
no hardcoded genre list to mismatch your metadata provider.

Genre pairs are first-class terms: Alien emits `blend:horror_science_fiction` as well as its
two genres, so "sci-fi horror" is a concept in its own right rather than the coincidence of
two labels. Because blends are rarer than their components, IDF weights them higher
automatically.

**3 · Ranking.** A per-user logistic regression, fitted on that user's own labels. It learns
that *this* user cares about the director and *that* one does not — something a global
slider cannot express. **There are no bias sliders in the UI**; there is nothing to tune.

**4 · List construction.** Maximal Marginal Relevance for genuine variety
(`λ·score − (1−λ)·maxSimilarityToAlreadyPicked`), plus calibration toward the user's own
genre mix. 1.x added `random.uniform(0, diversity)` to every score, which is noise, not
diversity.

**5 · Explanation.** Scoring against individual liked items (rather than one averaged
"vibe vector") means the plugin knows *which* item drove each match — so it can say
**"Because you watched Blade Runner."**

---

## Before you install

**Back up first.** This is a pre-release. Snapshot your Jellyfin `config` directory, or
test on a non-production instance.

What it will do on your server:

- create one virtual folder per user, per enabled type
- add those folder ids to the owning user's `EnabledFolders`, and remove them from everyone
  else's — **it reads the existing policy and changes only its own entries**, never
  `EnableAllFolders`, never a wholesale rebuild
- write `.strm` stubs and `.nfo` files under `<jellyfin-data>/jellydiscover/`

Music uses **symbolic links**, because Jellyfin does not play `.strm` files in music
libraries. On Windows that needs Developer Mode or a service account with the privilege.
Music is **off by default** for that reason.

---

## Install

1. Dashboard → Plugins → Repositories → add the repository URL from the release notes.
2. Install **JellyDiscover**, then restart Jellyfin.
3. Dashboard → Plugins → JellyDiscover: enable the types you want, then **Generate now**.
4. Watch progress in Dashboard → Scheduled Tasks → *Generate discovery libraries*.

Or install manually: unzip into `<jellyfin-config>/plugins/JellyDiscover/` and restart.

---

## Removing it

There are three teardown paths because Jellyfin cannot guarantee any single one runs.

**The reliable one:** config page → **Remove all JellyDiscover libraries**. This revokes
the user permissions, removes the virtual folders, and deletes the generated content — in
that order, so no user policy is ever left pointing at a deleted library (which is what
produced "ghost items" in 1.x). Do this **before** uninstalling.

Also available: disabling a media type removes just those libraries; the `OnUninstalling`
hook attempts a full teardown as a safety net. Every path is idempotent — if one dies
half-way, running it again finishes the job.

### Removing libraries left by JellyDiscover 1.x

Config page → **Preview what would be removed**. Nothing is deleted until you confirm.

Matching is on the exact invisible-character naming scheme 1.x generated (`U+3164` prefix,
one or more `U+200B` suffixes) — **never** on words like "Discover" or "Recommended". That
keyword match is precisely what made the old cleaner destroy hand-made libraries such as
"Recommended Classics", and the predicate is covered by tests that assert it does not match
any of them.

Old *content folders* are deliberately left on disk. They live outside this plugin's data
root and a stray recursive delete there is the exact failure this rewrite exists to prevent.
The preview shows their paths so you can remove them yourself.

---

## Building

```bash
dotnet test          # 50 tests, no Jellyfin required, ~1s
dotnet build -c Release
```

The shipped payload is `Jellyfin.Plugin.JellyDiscover.dll` and `JellyDiscover.Core.dll`.
Nothing else — every other assembly in the publish output is provided by the host, and
shipping a second copy risks a load conflict.

### Layout

```
src/JellyDiscover.Core/            the whole algorithm. zero dependencies.
src/Jellyfin.Plugin.JellyDiscover/ the host adapter. all Jellyfin coupling lives here.
tests/JellyDiscover.Core.Tests/    fast tests, no server needed
```

If you find yourself wanting to add a package reference to `Core`, the code you are writing
probably belongs in the plugin project.

---

## Not yet implemented

- **Semantic embeddings.** The design targets ONNX Runtime with `all-MiniLM-L6-v2` behind
  an interface, with BM25 as the default. Native library loading inside a per-plugin
  `AssemblyLoadContext` is a known failure mode and needs a spike before it ships. BM25 with
  a library-derived vocabulary is the v1 default and needs no native dependency at all.
- **Collaborative filtering.** Co-occurrence needs density; a four-person household
  generates almost none. Planned as a bonus term that activates once a server has enough
  users, not as a pillar.
- Trakt and Jellyseerr integration (the feature slots exist in the ranker).
- Email digests.

## Licence

MIT, as with 1.x. Jellyfin plugins must be GPLv3 or permissive; MIT satisfies that.
