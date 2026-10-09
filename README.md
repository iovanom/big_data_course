# Big Data — Laboratoare

Colecție de laboratoare pentru cursul **Big Data**: exerciții cu **PySpark** în
Jupyter și un studiu comparativ al formatelor de stocare (**JSON/CSV vs Parquet
vs Delta Lake**) pe un set de date real (Yelp Open Dataset).

---

## Structura repository-ului

```
.
├── env.yaml                     # Mediu conda/micromamba (pyspark, delta, jupyter, ...)
├── laborator_0/                 # Introducere în PySpark
│   ├── Big Data Lab Sheet 1.docx
│   ├── laborator.ipynb          # select / filter / groupBy / join / Spark SQL
│   ├── pyspark_test.ipynb       # verificare rapidă a sesiunii Spark
│   └── data/
│       ├── filme.csv            # 20 filme (movieId, title, genre)
│       └── ratinguri.csv        # 50 rating-uri (userId, movieId, rating)
├── laborator_1/                 # Stocare: JSON vs Parquet vs Delta Lake
│   ├── notebooks/
│   │   └── L01_storage_yelp.ipynb
│   └── data/                    # (ignorat în git — date mari, se descarcă separat)
│       ├── raw/                 # JSON brut (JSON Lines)
│       ├── parquet/
│       ├── parquet_partitioned/
│       └── delta/
└── data/                        # (ignorat în git — dataset-ul Yelp brut)
```

---

## Mediu de lucru

Mediul este definit în [`env.yaml`](env.yaml). Versiuni principale:

| Componentă | Versiune |
|------------|----------|
| Python     | 3.12.12  |
| PySpark    | 4.1.1    |
| OpenJDK    | 17.0.18  |
| pandas     | 2.3.3    |
| matplotlib | 3.10.0   |
| Delta Lake | `delta-spark` (instalat prin pip) |

Creare și activare mediu:

```bash
micromamba create -f env.yaml    # sau: conda env create -f env.yaml
micromamba activate Playground
jupyter lab
```

Fără conda, dependențele minime se instalează cu:

```bash
pip install pyspark==4.1.1 delta-spark pandas matplotlib jupyterlab
```

> PySpark are nevoie de un JDK 17 disponibil (`JAVA_HOME`). Notebook-ul din
> `laborator_1` îl detectează automat în mediul activ.

---

## Datele (datasets)

**Nu sunt incluse în git** (fișiere de ordinul GB). Sunt ignorate prin
`.gitignore`:

- `data/` — dataset-ul Yelp la rădăcină (`Yelp-JSON.zip` și fișierele
  `yelp_academic_dataset_*.json`);
- `laborator_1/data/` — datele brute pentru laboratorul 1 plus derivatele
  Parquet / Parquet partiționat / Delta Lake.

Dataset-ul folosit este **Yelp Open Dataset**, disponibil gratuit la
<https://www.yelp.com/dataset> (necesită acceptarea acordului de utilizare).
Fișierul relevant pentru laboratorul 1 este
`yelp_academic_dataset_review.json` (~5 GB), care trebuie plasat în
`data/` sau în `laborator_1/data/raw/`.

Fișierele CSV mici din `laborator_0/data/` (filme și rating-uri) **sunt**
incluse în repo, fiind necesare pentru rularea notebook-ului.

---

## Laborator 0 — Introducere în PySpark

Scop: configurarea Jupyter + PySpark și exersarea operațiilor de bază pe un
dataset mic generat cu AI.

- **`laborator.ipynb`** — citește `data/filme.csv` și `data/ratinguri.csv` și
  demonstrează:
  - `select` și `filter` (filme după gen, rating-uri ≥ 4.5);
  - `groupBy` + agregări (`avg`, `count`) — rating mediu / număr de rating-uri
    per film și per gen;
  - `join` între filme și rating-uri;
  - **Spark SQL** (`createOrReplaceTempView` + `spark.sql`) și o interogare cu
    fereastră (`ROW_NUMBER`) pentru top 3 genuri per utilizator.
- **`pyspark_test.ipynb`** — verificare rapidă a unei sesiuni Spark locale
  (`local[*]`), transformări și acțiuni pe un DataFrame construit în memorie.
- **`Big Data Lab Sheet 1.docx`** — fișa de laborator (cerințe, pași).

---

## Laborator 1 — JSON vs Parquet vs Delta Lake

Scop: compararea formatelor de stocare pe un set real de recenzii Yelp, urmărind
**spațiul pe disc** și **timpii de interogare**.

Notebook: **`laborator_1/notebooks/L01_storage_yelp.ipynb`**. Pași principali:

1. Configurarea mediului (Java, sesiune Spark cu Delta Lake) și verificarea
   funcționării worker-ului Python și a Delta Lake.
2. Citirea JSON-ului cu **schemă explicită** (JSON Lines), adăugarea coloanei
   derivate `year`.
3. Două interogări comune tuturor formatelor:
   - **filtrare** — recenzii de 5 stele din anul 2019;
   - **agregare** — pe ani ≥ 2015: număr de recenzii, media stelelor și media
     voturilor `useful`.
4. Scrierea și recitirea datelor ca **Parquet**, apoi ca **Parquet partiționat**
   pe `year`.
5. **Delta Lake** partiționat, cu crearea de versiuni:
   `WRITE` (v0) → `APPEND` (v1) → `UPDATE` (v2) → `DELETE` (v3).
6. **Time travel** (`versionAsOf = 0`), istoricul tranzacțiilor și inspectarea
   directorului `_delta_log/`.
7. Concluzii pe baza valorilor măsurate: compresie, *partition pruning*
   (`PartitionFilters`) și avantajele tranzacționale ale Delta Lake.

Rezultatele sunt măsurate cu mediană peste mai multe rulări (`REPEATS`), pentru
a reduce influența cache-ului sistemului de operare.

---

## Rulare

```bash
# Laboratorul 0
jupyter lab laborator_0/laborator.ipynb

# Laboratorul 1 (necesită yelp_academic_dataset_review.json)
jupyter lab laborator_1/notebooks/L01_storage_yelp.ipynb
```

Notebook-ul din `laborator_1` creează automat directoarele `raw/`, `parquet/`,
`parquet_partitioned/` și `delta/` dacă lipsesc.

---

## Note

- Datele mari nu sunt versionate; pentru a reproduce rezultatele trebuie
  descărcat dataset-ul Yelp și rerulate celulele.
- Fișierele `_delta_log/`, `*.crc` și `spark-warehouse/` sunt artefacte de
  rulare și sunt ignorate.
