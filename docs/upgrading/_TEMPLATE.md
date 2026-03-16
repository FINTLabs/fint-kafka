# Oppgradering: `v<fra-major>` -> `v<til-major>`

> Eksakte tagger for dette hoppet: `<fra-tag> -> <til-tag>`.

Kort oppsummering av hva som er breaking og hva som må gjøres i konsumerende applikasjoner.

## 1. Scope

- Fra major: `v<fra-major>`
- Til major: `v<til-major>`
- Eventuelle versjoner som hoppes over: `<eks. v5.0.0>`

## 2. Hurtigoversikt

| Område | Endring | Påvirkning |
|---|---|---|
| Konfigurasjon |  |  |
| API (klasser/metoder) |  |  |
| Oppførsel/runtime |  |  |
| Dependency/plattform |  |  |

## 3. Konfigurasjonsendringer

### 3.1 Rename/flytting av variabler

| Gammel | Ny | Kommentar |
|---|---|---|
|  |  |  |

### 3.2 Fjernede variabler

- `...`

### 3.3 Nye variabler

- `...`

## 4. API-endringer

### 4.1 Klasser og services

| Gammel | Ny |
|---|---|
|  |  |

### 4.2 Metodesignaturer

- `oldMethod(...)` -> `newMethod(...)`

### 4.3 Kodeeksempler (gammel -> ny)

```java
// gammel kode
```

```java
// ny kode
```

## 5. Endret oppførsel

- Punkt 1
- Punkt 2

## 6. Sjekkliste

1. Oppdater dependency.
2. Oppdater konfig.
3. Oppdater imports/API-bruk.
4. Verifiser runtime-oppførsel i test.
