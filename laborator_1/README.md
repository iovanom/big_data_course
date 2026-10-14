## Pornire
1. `docker compose up -d`  (Kafka UI: http://localhost:8080, mongo-express: http://localhost:8081)
2. `micromamba activate Playground`
3. Descărcați Yelp Open Dataset: review.json -> `data/raw/`, business.json + user.json -> `data/raw_dims/`
4. Rulați `notebooks/L01_yelp_bigdata.ipynb` (Restart & Run All). Etapa 4 pornește singură streaming-ul și producer-ul.
5. Dashboard: `streamlit run dashboard/app.py`
6. Demo live: `./scripts/reset_stream.sh`, apoi în terminale separate:
   `python scripts/streaming_job.py` și `python scripts/producer_kafka.py --pause 3`

## Setul de date

### Setul de date: Yelp Open Dataset

Laboratorul folosește **[Yelp Open Dataset](https://business.yelp.com/data/resources/open-dataset/)**, un subset de date reale publicat de Yelp pentru uz educațional. Conține afaceri locale, recenzii, utilizatori, check-in-uri și fotografii.

| Indicator (conform paginii oficiale) | Valoare |
|---|---|
| Recenzii | 6.990.280 |
| Afaceri | 150.346 |
| Zone metropolitane | 11 |
| Arhiva JSON | 4,35 GB comprimată, 8,65 GB dezarhivată (5 fișiere JSON + 1 PDF cu documentația) |

**Descărcare:** [pagina Yelp Open Dataset](https://business.yelp.com/data/resources/open-dataset/), butonul „Download JSON” ([link direct](https://business.yelp.com/external-assets/files/Yelp-JSON.zip)). Arhiva cu fotografii nu e necesară pentru acest laborator.

#### Fișiere folosite în proiect

Fiecare fișier are format JSON Lines: un obiect JSON pe fiecare linie.

| Fișier | Unde se pune | Rol în laborator | Câmpuri folosite |
|---|---|---|---|
| `yelp_academic_dataset_review.json` (~5,09 GB) | `data/raw/` | sursa principală (Etapele 1–4), tabelul de fapte `fact_review` | `review_id`, `user_id`, `business_id`, `stars`, `date`, `text`, `useful`, `funny`, `cool` |
| `yelp_academic_dataset_business.json` | `data/raw_dims/` | join în Etapa 2, dimensiunea `dim_business` | `business_id`, `name`, `city`, `state`, `stars`, `review_count`, `is_open`, `categories` |
| `yelp_academic_dataset_user.json` | `data/raw_dims/` | dimensiunea `dim_user` | `user_id`, `review_count`, `yelping_since`, `fans`, `average_stars` |

`checkin.json` și `tip.json` fac parte din arhivă, dar nu sunt folosite.

> **Atenție:** în `data/raw/` trebuie să stea **doar** `review.json`. Etapa 1 citește tot directorul (`spark.read.json(RAW_DIR)`).

#### Observații despre date, relevante pentru procesare

- **Chei:** `review.business_id` → `business.business_id`, `review.user_id` → `user.user_id`. Toate ID-urile sunt șiruri de 22 de caractere.
- **`categories`** este un string cu valori separate prin virgulă, nu o listă. `main_category` = prima etichetă, iar ordinea etichetelor nu e garantată de Yelp.
- **Nume de orașe nenormalizate:** de exemplu, `Saint Louis` / `St. Louis` / `St Louis` (MO).
- **Anul 2022 e incomplet:** setul se oprește în ianuarie 2022, cu ~31,6 mii recenzii. De aceea, analiza pe ani se limitează la recenziile din `year <= 2021`.
- **Timpul:** `date` din recenzii conține și ora. Toate calculele se fac în UTC (vezi `src/lab_common.py`).

#### Licență și redistribuire

Setul e pus la dispoziție de Yelp Inc. pentru uz educațional și academic, în condițiile acordului inclus în arhivă. Datele **nu sunt incluse în acest repository**: `data/` este în `.gitignore`. Fiecare utilizator trebuie să le descarce singur de pe pagina oficială.
````

Ce trebuie să mai verifici:
- **Licența:** am văzut doar că pagina oficială descrie setul ca destinat uzului educațional. Textul exact al acordului nu l-am citit. Verifică PDF-ul din arhivă și, dacă vrei, pune numele lui exact în README.
- **Linkul direct `Yelp-JSON.zip`:** l-am luat de pe butonul de pe pagină. Pagina spune că descărcarea e o arhivă TAR, dar linkul se termină în `.zip`. Dacă linkul se schimbă, rămâne valabil cel către pagina principală.
- **Cifrele:** 5,09 GB și 31,6 mii pentru 2022 vin din analizele tale anterioare, nu de pe site-ul Yelp.
