# Oppgraderingsguider (major versjoner)

Denne mappen inneholder oppgraderingsguider per major-hopp.

## Struktur

- En fil per hopp: `vX-to-vY.md`
- En fast mal for nye hopp: [`_TEMPLATE.md`](_TEMPLATE.md)
- Denne filen er indeksen

## Tilgjengelige guider

| Fra | Til | Guide |
|---|---|---|
| `v3` | `v4` | [`v3-to-v4.md`](v3-to-v4.md) |
| `v4` | `v6` | [`v4-to-v6.md`](v4-to-v6.md) |

Fotnoter (eksakte tagger):

- `v3 -> v4`: `v3.2.0-rc-3 -> v4.0.3`
- `v4 -> v6`: `v4.0.3 -> v6.0.0`

## Hvordan legge til ny major-guide (v7, v8, ...)

1. Kopier [`_TEMPLATE.md`](_TEMPLATE.md) til `v<forrige-major>-to-v<ny-major>.md`.
2. Fyll ut dokumentet basert på kode-diff mellom relevante tagger.
3. Oppdater tabellen i denne indeksen med ny rad.
4. Oppdater lenken i hoved-README hvis nødvendig.

## Anbefalt arbeidsmåte for hver ny guide

1. Dokumenter konfig-endringer (`application.yml`/env).
2. Dokumenter API-endringer (klasse/service/metode-navn).
3. Dokumenter endret oppførsel (runtime/semantikk).
4. Legg inn gamle vs nye kodeeksempler der migrering ikke er triviel.
5. Avslutt med en kort sjekkliste.
