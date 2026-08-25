# JellyDiscover

Personalised recommendation libraries for every user on your Jellyfin server.

JellyDiscover 2.0 is a **complete rewrite** — now a native Jellyfin plugin instead of a
standalone Python app. It replaces the deprecated 1.x engine entirely.

Each user gets their own **Discover Movies / Discover Shows / Discover Music** library,
visible only to them, refreshed as they watch. Because a library is a server-side object,
it renders on every client — web, Android TV, Roku, Swiftfin, Kodi — with no client-side
support required.

> **⚠️ Alpha — not yet tested against a live server.** The recommendation algorithm is
> covered by 50 unit tests, but the Jellyfin integration layer has **never been run on an
> actual server**. Expect rough edges, missing error handling, and possible breakage.
> **Do not install this on a production Jellyfin instance.** Use a test server or snapshot
> your config directory first.

---

## What's different in 2.0

JellyDiscover now runs as a native Jellyfin plugin instead of a standalone app. No separate
installer, no external dashboard, no path mapping — it lives inside Jellyfin and uses the
same APIs, filesystem, and authentication that Jellyfin itself does.

| | What you get |
|---|---|
| **Install** | Paste a repo URL in the Jellyfin dashboard, click install |
| **Size** | 2 DLLs, 174 KB total |
| **Config** | Built into the Jellyfin dashboard, inherits admin auth |
| **Scheduling** | Uses Jellyfin's own task scheduler |
| **Catalogue** | Processes your entire library, not a capped subset |
| **Uninstall** | One button to remove everything, plus a safety-net uninstall hook |

---

## How the recommendations work

Five stages, all in `JellyDiscover.Core`, which has **zero dependencies** — no Jellyfin,
no I/O, no clock. That is what makes the whole algorithm testable in milliseconds.

**1 · Signals.** Labels are derived from your Jellyfin watch data:

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
genre mix.

**5 · Explanation.** Scoring against individual liked items (rather than one averaged
"vibe vector") means the plugin knows *which* item drove each match — so it can say
**"Because you watched Blade Runner."**

---

## Before you install

**This is alpha software.** It compiles, the algorithm passes its tests, but it has never
been loaded into a running Jellyfin server. There will be bugs. Back up your Jellyfin
`config` directory, or — better — test on a throwaway instance.

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
that order, so no user policy is ever left pointing at a deleted library. Do this
**before** uninstalling.

Also available: disabling a media type removes just those libraries; the `OnUninstalling`
hook attempts a full teardown as a safety net. Every path is idempotent — if one dies
half-way, running it again finishes the job.

### Removing libraries left by a previous version

Config page → **Preview what would be removed**. Nothing is deleted until you confirm.

Matching uses the exact invisible-character naming scheme from the old version (`U+3164`
prefix, one or more `U+200B` suffixes) — **never** broad keywords like "Discover" or
"Recommended", so your hand-made libraries are safe. The matching predicate is covered by
tests.

Old content folders on disk are deliberately left alone. The preview shows their paths so
you can remove them yourself.

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

MIT. Jellyfin plugins must be GPLv3 or permissive; MIT satisfies that.
