# Exchange Naptár Home Assistant integráció

[![hacs_badge](https://img.shields.io/badge/HACS-Custom-41BDF5.svg)](https://github.com/hacs/integration)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Home Assistant custom integráció Microsoft Exchange naptárakhoz.

Támogatja az **on-premise Exchange** (NTLM / Basic / ügyfél-tanúsítvány, EWS-en keresztül) és az **Office 365** (Microsoft Graph API-n keresztül) hozzáférést teljes CRUD műveletekkel, fiókonként több naptárral és múltbeli események böngészésével.

> A [MMM-Exchange](https://github.com/bohemtucsok/MMM-Exchange) MagicMirror modul alapján, Python/Home Assistant-ra portolva.

## Funkciók

- Naptáresemények **olvasása** automatikus ismétlődő esemény kibontással
- Események **létrehozása**, **módosítása** és **törlése** Home Assistant-ból (opcionális csak olvasható mód)
- **Több naptár fiókonként** — minden kiválasztott naptár külön `calendar.*` entitás
- **Múltbeli események böngészése** — a naptárpanel visszafelé is lapozható, nem csak előre
- On-premise Exchange (NTLM hitelesítés)
- Basic EWS hitelesítés (AWS WorkMail és hasonló)
- **Tanúsítvány-alapú hitelesítés** (ügyfél-tanúsítványos TLS) vállalati on-premise Exchange-hez
- Office 365 / Microsoft 365 (Microsoft Graph API)
- Önaláírt SSL tanúsítvány támogatás
- **Hitelesítő adatok cseréje újra-hozzáadás nélkül** — automatikus újrahitelesítés lejárt jelszó, titkos kulcs vagy tanúsítvány esetén, plusz proaktív Újrakonfigurálás
- Cache-first naptárnézet, nincs villogás átmeneti hibánál, hibás események robusztus kezelése
- Extra esemény-attribútumok (`free_busy_status`, `sensitivity`, `categories`) automatizmusokhoz
- Beállítható lekérdezési időköz, dátumtartomány, eseménykorlát és egyéni User-Agent
- **Hangasszisztens támogatás** (Home Assistant Voice PE / Assist pipeline)
- Magyar és angol UI fordítások
- HACS kompatibilis

## Telepítés

### HACS (Ajánlott)

1. Nyisd meg a HACS-ot a Home Assistant-ban
2. Kattints a három pontos menüre (jobb fent) > **Egyéni tárhelyek**
3. Add hozzá a repository URL-t: `https://github.com/bohemtucsok/homeassistant-exchange-calendar`
4. Kategória: **Integráció**
5. Kattints a **Hozzáadás**-ra, majd keresd meg az "Exchange Calendar"-t és telepítsd
6. Indítsd újra a Home Assistant-ot

### Kézi telepítés

1. Másold a `custom_components/exchange_calendar/` mappát a Home Assistant `config/custom_components/` könyvtárába
2. Indítsd újra a Home Assistant-ot

## Beállítás

### On-premise Exchange (NTLM)

1. Menj a **Beállítások** > **Eszközök és Szolgáltatások** > **Integráció hozzáadása**
2. Keresd meg az "Exchange Calendar"-t
3. Válaszd az **On-premise (NTLM)** opciót
4. Töltsd ki:
   - **Exchange szerver hosztnév**: pl. `mail.example.com`
   - **E-mail cím**: Az e-mail címed (pl. `felhasznalo@example.com`)
   - **Felhasználónév**: (Opcionális) Ha különbözik az e-mail címtől
   - **Jelszó**: A jelszavad
   - **Windows domain**: (Opcionális) pl. `MYDOMAIN`
   - **Nem biztonságos SSL engedélyezése**: Önaláírt tanúsítványokhoz
5. Állítsd be a naptár opciókat (napok száma, max események, frissítési időköz)

> **MMM-Exchange felhasználóknak**: A konfigurációs mezők azonos logikát követnek:
> - `host` -> Exchange szerver hosztnév
> - `username` -> E-mail / Felhasználónév
> - `password` -> Jelszó
> - `domain` -> Windows domain
> - `allowInsecureSSL` -> Nem biztonságos SSL

### On-premise Exchange (Tanúsítvány-alapú hitelesítés)

Olyan Exchange szerverekhez, amelyek jelszó helyett ügyfél-tanúsítványos hitelesítést követelnek (egyes vállalati környezetekben gyakori).

#### Előfeltételek

1. Exportáld az ügyfél-tanúsítványt PEM fájlba, amely **a tanúsítványt és a privát kulcsot is** tartalmazza.
   - Ha a tanúsítvány PFX formátumú, előbb konvertáld:
     ```bash
     openssl pkcs12 -in certificate.pfx -out client.pem -nodes
     ```
2. A privát kulcs **ne legyen jelszóval védve**. Ha az, távolítsd el:
   ```bash
   openssl rsa -in client.pem -out client_unencrypted.pem
   ```
   Ezután a `client_unencrypted.pem` fájlt használd tanúsítványként.
3. Tedd a PEM fájlt a Home Assistant számára elérhető helyre (pl. `/config/ssl/exchange.pem`), és védd: `chmod 600 /config/ssl/exchange.pem`.

#### Home Assistant beállítás

1. Menj a **Beállítások** > **Eszközök és Szolgáltatások** > **Integráció hozzáadása**
2. Keresd meg az "Exchange Calendar"-t
3. Válaszd az **On-premise (Tanúsítvány)** opciót
4. Töltsd ki:
   - **Exchange szerver hosztnév**: pl. `mail.example.com`
   - **E-mail cím**: Az e-mail címed (pl. `felhasznalo@example.com`)
   - **Ügyfél-tanúsítvány útja**: A PEM fájl abszolút elérési útja (pl. `/config/ssl/exchange.pem`)
   - **Privát kulcs útja** (opcionális): A legtöbb esetben **hagyd üresen**. Csak akkor töltsd ki, ha a privát kulcs külön fájlban van.
   - **Nem biztonságos SSL engedélyezése**: Önaláírt szervertanúsítványokhoz (a kliens-tanúsítványt ilyenkor is bemutatja)
5. Állítsd be a naptár opciókat

> **Újrahitelesítés**: Ha a tanúsítvány lejár, az integráció **Újrakonfigurálás** vagy **Újrahitelesítés** menüjében frissítheted a tanúsítvány útját az integráció eltávolítása nélkül. A tanúsítványos kapcsolat a Beállításokban megadott egyéni **User-Agentet** is használja. Külön kulcsfájl esetén az integráció a tanúsítványt és a kulcsot egy privát ideiglenes fájlban egyesíti a konfiguráció élettartamára.

### Office 365 (Graph API)

Microsoft Graph API-t használ Office 365 / Microsoft 365 postafiókokhoz.

#### Előfeltétel: Azure AD alkalmazás regisztráció

1. Menj az [Azure Portal](https://portal.azure.com) > **Azure Active Directory** > **Alkalmazásregisztrációk**
2. Kattints az **Új regisztráció**-ra
   - Név: `Home Assistant Exchange Calendar`
   - Támogatott fióktípusok: **Egybérlős**
3. A létrehozás után jegyezd fel az **Alkalmazás (Ügyfél) azonosítót** és a **Könyvtár (Bérlő) azonosítót**
4. Menj a **Tanúsítványok és titkos kulcsok** > **Új titkos kulcs**
   - Jegyezd fel az **Értéket** (ez a Client Secret)
5. Menj az **API engedélyek** > **Engedély hozzáadása**
   - Válaszd a **Microsoft Graph** > **Alkalmazás engedélyek**
   - Add hozzá: `Calendars.ReadWrite` és `User.Read.All`
   - Kattints az **Rendszergazdai jóváhagyás megadása** gombra mindkét engedélyhez

#### Home Assistant beállítás

1. Menj a **Beállítások** > **Eszközök és Szolgáltatások** > **Integráció hozzáadása**
2. Keresd meg az "Exchange Calendar"-t
3. Válaszd az **Office 365 (Graph API)** opciót
4. Töltsd ki:
   - **E-mail cím**: A postafiók e-mail címe
   - **Azure AD Bérlő azonosító (Tenant ID)**: Az alkalmazásregisztrációból
   - **Alkalmazás azonosító (Client ID)**: Az alkalmazásregisztrációból
   - **Titkos kulcs (Client Secret)**: Az alkalmazásregisztrációból
5. Állítsd be a naptár opciókat

> **Frissítés v1.x-ről (EWS/OAuth2)?** Hozzá kell adnod a `User.Read.All` Application engedélyt az Azure AD alkalmazásodhoz és meg kell adnod a rendszergazdai jóváhagyást. A meglévő konfiguráció továbbra is működik.

## Használat

### Naptár kártya

Adj hozzá egy naptár kártyát a dashboard-hoz:

```yaml
type: calendar
entities:
  - calendar.exchange_felhasznalo_example_com
```

### Szolgáltatások

#### Esemény létrehozása
```yaml
service: calendar.create_event
target:
  entity_id: calendar.exchange_felhasznalo_example_com
data:
  summary: "Csapat megbeszélés"
  start_date_time: "2025-03-01 10:00:00"
  end_date_time: "2025-03-01 11:00:00"
  description: "Heti szinkron"
  location: "Tárgyaló A"
```

### Automatizációk

Naptár események használata triggerként:

```yaml
automation:
  - alias: "Megbeszélés emlékeztető"
    trigger:
      - platform: calendar
        event: start
        entity_id: calendar.exchange_felhasznalo_example_com
        offset: "-00:15:00"
    action:
      - service: notify.mobile_app
        data:
          message: "A megbeszélés 15 perc múlva kezdődik!"
```

### Hangasszisztens (Voice PE / Assist)

Az integráció kompatibilis a Home Assistant Assist pipeline-nal, így hangvezérléssel is lekérdezhetők a naptáresemények:

- **"Milyen programom van holnap?"** - Események lekérdezése természetes nyelven
- **"Mi van a naptáramban jövő héten?"** - Relatív dátumok támogatása

Az események időpontjai automatikusan helyi időzónára konvertálódnak, így a hangasszisztens mindig a helyes időt mondja.

> **Tipp**: Az OpenAI Conversation integráció (gpt-4o) használatával a legjobb az élmény. A `gpt-4o-mini` modell időnként pontatlan a dátumszámításoknál.

## Opciók

A kezdeti beállítás után módosíthatod az opciókat: **Beállítások** > **Eszközök és Szolgáltatások** > **Exchange Calendar** > **Beállítás**:

| Opció | Alapérték | Leírás |
|--------|---------|---------|
| Megjelenítendő naptárak | csak az elsődleges | A postafiók mely naptárai jelenjenek meg entitásként (többválasztós; mindegyik külön `calendar.*` entitás) |
| Előrejelzett napok száma | 30 | A cache-elt előre eső ablak mérete, a mai nap kezdetétől számítva (30–90). Az ebbe eső kérések a cache-ből szolgálódnak ki |
| Események maximális száma | 50 | Naptáranként a cache-elt ablak korlátja; az ennél több eseményű naptár élő lekérdezésre vált |
| Frissítési időköz | 5 perc | Milyen gyakran kérdezze le az Exchange szervert |
| Csak olvasható mód | ki | Letiltja a létrehozást/módosítást/törlést az entitásokon |
| Egyéni User-Agent | üres | Felülírja az `exchangelib` User-Agentet olyan szerverekhez, amelyek blokkolják (a HA példány összes EWS-kapcsolatára hat) |

## Hibakezelés

### Nem tud csatlakozni az Exchange szerverhez
- Ellenőrizd, hogy a szerver hosztnév helyes és elérhető a HA-ból
- On-premise: győződj meg, hogy az EWS végpont elérhető (`https://server/EWS/Exchange.asmx`)
- Önaláírt tanúsítványokhoz: engedélyezd a "Nem biztonságos SSL"-t
- Ellenőrizd a HA logokat részletes hibaüzenetekért

### Hitelesítési hiba
- NTLM: Próbáld mind a `user@domain.com` és a `DOMAIN\user` formátumokat
- OAuth2: Ellenőrizd, hogy megadtad a rendszergazdai jóváhagyást a `Calendars.ReadWrite`-hoz
- OAuth2: Győződj meg, hogy a client secret nem járt le
- Tanúsítvány: a PEM tartalmazza a tanúsítványt **és** a titkosítatlan privát kulcsot (vagy add meg külön a "Privát kulcs útja" mezőben); az út legyen abszolút és a Home Assistant számára olvasható
- Tanúsítvány: ha lejárt, az **Újrahitelesítés** / **Újrakonfigurálás** menüben mutass a megújított fájlra
- `401 Unauthorized` IIS/NTLM mögött: próbálj egyéni **User-Agentet** a Beállításokban (lásd lent)

### Nincsenek események
- Ellenőrizd, hogy a postafiókban vannak események a beállított időtartományban
- Növeld az "Előrejelzett napok száma" értékét a beállításokban
- Ellenőrizd, hogy az e-mail cím a postafiókhoz tartozik
- Másodlagos naptárat keresel? Válaszd ki a **Beállítás** → **Megjelenítendő naptárak** alatt
- A korábbi hónapokat élőben kéri le a szerverről; ha ez nem sikerül, a panel csak a cache-elt (közelgő) eseményeket mutatja — nézd meg a HA naplót kapcsolati hibákért

### Lassú a naptárpanel vagy "nem tölthetők be az események"
- A cache-elt ablakon belüli kérések azonnaliak; csak az azon kívüli tartományok (pl. korábbi hónapok) érik el élőben a szervert
- Növeld az "Események maximális száma" értékét, ha egy zsúfolt naptár túllépi — különben az a naptár mindig élőben kérdez
- Egyes szerverek korlátozzák vagy blokkolják az alapértelmezett `exchangelib` User-Agentet — állíts be egyénit a Beállításokban

## Biztonsági szempontok

- Mindig HTTPS-t használj az Exchange szerverhez való csatlakozáskor
- On-premise NTLM kapcsolatoknál erősen ajánlott az Exchange-et megbízható belső hálózaton vagy VPN-en keresztül elérni
- Lehetőség szerint használj dedikált szolgáltatásfiókot minimális jogosultságokkal
- Az ügyfél-tanúsítvány PEM fájlját tartsd privátan (`chmod 600`) — a privát kulcsodat tartalmazza. Külön kulcsfájl esetén az integráció egy privát ideiglenes fájlban tart egy egyesített másolatot a konfiguráció élettartamára
- A "Nem biztonságos SSL engedélyezése" kikapcsolja a szervertanúsítvány ellenőrzését az adott kapcsolatra — csak önaláírt tanúsítványokhoz, megbízható hálózaton használd

## Teljesítmény, robusztusság és extra attribútumok

- **Cache-first naptárnézet** — az integráció egy előre eső esemény-ablakot tart fenn (a mai nap kezdetétől, `Előrejelzett napok száma` napra). Az ebbe az ablakba eső naptárpanel-kérések azonnal a cache-ből szolgálódnak ki, élő Exchange-hívás nélkül. Az ablakon kívüli tartományok (pl. korábbi hónapok) továbbra is élőben kérdeződnek le, így a múltbeli böngészés működik. Ha egy naptárban több esemény van, mint az `Események maximális száma`, az integráció arra a naptárra élő lekérdezésre vált — emeld a limitet a teljes cache-lefedettséghez.
- **Nincs villogás átmeneti hibánál** — ha egy naptár frissítése nem sikerül, az utoljára ismert események megmaradnak. Ha a szerver teljesen elérhetetlen, az entitások `unavailable` állapotba kerülnek, de az utolsó adatot megőrzik.
- **Robusztus eseménykezelés** — a hibás Exchange-események (`end` a `start` előtt, egész napos esemény exkluzív vég nélkül, vegyes dátum/időpont határok, időzóna nélküli időbélyegek, túl hosszú tárgy) normalizálva vannak, így nem törik meg a naptár felületét.
- **Extra attribútumok** — minden naptár-entitás kiteszi az aktuális/következő esemény `free_busy_status`, `sensitivity` és `categories` mezőit, ami értesítés-automatizmusokhoz hasznos.

### Egyéni User-Agent

Egyes on-premise Exchange szerverek blokkolják vagy korlátozzák az alapértelmezett `exchangelib` User-Agentet (pl. `401 Unauthorized` IIS/NTLM mögött). Egyéni értéket az integráció **Beállítások** (Options) menüjében adhatsz meg, pl. `Microsoft Outlook/16.0 (Android; en-US)`. Megjegyzés: az exchangelib a User-Agentet folyamat-szinten alkalmazza, így a Home Assistant példány összes EWS-kapcsolatára hat. Hagyd üresen az alapértelmezéshez.

## Követelmények

- Home Assistant 2024.1.0 vagy újabb
- Hálózati hozzáférés az Exchange szerverhez (on-premise) vagy Office 365-höz
- Python könyvtár: `exchangelib` (automatikusan települ)

## Fejlesztési terv

- [x] HACS integráció
- [x] On-premise Exchange támogatás (NTLM)
- [x] Office 365 támogatás (OAuth2) EWS-en keresztül
- [x] Csak olvasható mód
- [x] Basic EWS hitelesítés (AWS WorkMail)
- [x] Hangasszisztens (Assist pipeline) támogatás
- [x] **Microsoft Graph API migráció Office 365-höz** — Az Office 365 mostantól Graph API-t használ EWS helyett. Az on-premise (NTLM/Basic) továbbra is EWS-t használ. Lásd [#3](https://github.com/bohemtucsok/homeassistant-exchange-calendar/issues/3).
- [x] Múltbeli események böngészése — A naptár nézet mostantól támogatja a múltbeli események megtekintését
- [x] **Több naptár támogatás fiókonként** — A postafiók további naptárai külön entitásként jeleníthetők meg. Az integráció **Beállítások** (Options) menüjében pipálhatók ki; minden kiválasztott naptár külön `calendar.*` entitás lesz.
- [x] **Hitelesítő adatok cseréje újra-hozzáadás nélkül** — automatikus újrahitelesítés lejárt jelszó, titkos kulcs vagy tanúsítvány esetén, plusz proaktív Újrakonfigurálás. Lásd [#12](https://github.com/bohemtucsok/homeassistant-exchange-calendar/issues/12).
- [x] **Teljesítmény és robusztusság** — cache-first naptárnézet, villogásmentes frissítés, hibás események normalizálása, extra esemény-attribútumok.
- [x] **Tanúsítvány-alapú hitelesítés (CBA)** on-premise Exchange-hez — [#16](https://github.com/bohemtucsok/homeassistant-exchange-calendar/pull/16) közreműködésként; jelenleg közösségi tesztelés alatt.
- [x] Egyéni User-Agent opció
- [ ] Exchange feladatok megjelenítése Home Assistant feladatlista entitásként
- [ ] Megosztott / szoba naptár támogatás
- [ ] Személyes Microsoft fiók támogatás

## Támogatók

<p align="center">
  <a href="https://infotipp.hu"><img src="docs/images/infotipp-logo.png" height="40" alt="Infotipp Rendszerház Kft." /></a>
  &nbsp;&nbsp;&nbsp;&nbsp;
  <a href="https://brutefence.com"><img src="docs/images/brutefence.png" height="40" alt="BruteFence" /></a>
</p>

## Licenc

MIT License - lásd [LICENSE](LICENSE).
