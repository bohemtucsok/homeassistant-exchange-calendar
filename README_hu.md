# Exchange Naptár Home Assistant integráció

[![hacs_badge](https://img.shields.io/badge/HACS-Custom-41BDF5.svg)](https://github.com/hacs/integration)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Home Assistant custom integráció Microsoft Exchange naptárakhoz EWS (Exchange Web Services) protokollon keresztül.

Támogatja az **on-premise Exchange** (NTLM) és az **Office 365** (OAuth2) hozzáférést teljes CRUD műveletekkel.

> A [MMM-Exchange](https://github.com/bohemtucsok/MMM-Exchange) MagicMirror modul alapján, Python/Home Assistant-ra portolva.

## Funkciók

- Naptáresemények **olvasása** automatikus ismétlődő esemény kibontással
- Új események **létrehozása** Home Assistant-ból
- Meglévő események **módosítása**
- Események **törlése**
- On-premise Exchange (NTLM hitelesítés)
- Basic EWS hitelesítés (AWS WorkMail és hasonló)
- Office 365 / Microsoft 365 (OAuth2 hitelesítés)
- Önaláírt SSL tanúsítvány támogatás
- Beállítható lekérdezési időköz, dátumtartomány és esemény korlátok
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

### Office 365 (OAuth2)

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
   - Add hozzá: `Calendars.ReadWrite`
   - Kattints az **Rendszergazdai jóváhagyás megadása** gombra

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
| Előrejelzett napok | 14 | Hány napra előre kérdezze le az eseményeket |
| Max események | 50 | Megjelenített események maximális száma |
| Frissítési időköz | 5 perc | Milyen gyakran kérdezze le az Exchange szervert |

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

## Biztonsági szempontok

- Mindig HTTPS-t használj az Exchange szerverhez való csatlakozáskor
- On-premise NTLM kapcsolatoknál erősen ajánlott az Exchange-et megbízható belső hálózaton vagy VPN-en keresztül elérni
- Lehetőség szerint használj dedikált szolgáltatásfiókot minimális jogosultságokkal

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
- [ ] **Microsoft Graph API migráció Office 365-höz** — Az EWS [2026 októberében megszűnik Exchange Online-on](https://learn.microsoft.com/en-us/exchange/clients-and-mobile-in-exchange-online/deprecation-of-ews-exchange-online). Az on-premise (NTLM) nem érintett. Lásd [#3](https://github.com/bohemtucsok/homeassistant-exchange-calendar/issues/3).
- [ ] Exchange feladatok megjelenítése Home Assistant feladatlista entitásként
- [ ] Megosztott / szoba naptár támogatás
- [ ] Több naptár támogatás fiókonként
- [ ] Személyes Microsoft fiók támogatás

## Támogatók

<p align="center">
  <a href="https://infotipp.hu"><img src="docs/images/infotipp-logo.png" height="40" alt="Infotipp Rendszerház Kft." /></a>
  &nbsp;&nbsp;&nbsp;&nbsp;
  <a href="https://brutefence.com"><img src="docs/images/brutefence.png" height="40" alt="BruteFence" /></a>
</p>

## Licenc

MIT License - lásd [LICENSE](LICENSE).
