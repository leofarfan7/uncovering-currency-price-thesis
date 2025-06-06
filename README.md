# Uncovering the Currency Price in an Unsustainable Fixed Exchange Rate Regime Using a Stablecoin: The Case of Bolivia

_A companion repository for the BSc thesis_ [_“Uncovering the Currency Price in an Unsustainable Fixed Exchange-Rate
Regime”_](https://university-domain.edu/path/to/thesis.pdf)

![Python 3.10+](https://img.shields.io/badge/python-%E2%89%A53.10-blue)
![MongoDB](https://img.shields.io/badge/database-mongodb-green)
![Licence](https://img.shields.io/badge/license-MIT-lightgrey)

> **Goal** – Harvest, clean and store minute-level stable-coin (USDT/BOB, USDT/ARS) quotes, the CMV bank rate,
> TradingView benchmarks and street-cash prices scraped from 10+ Bolivian newspapers, then aggregate them into tidy
> datasets that drive all the figures and econometrics in the thesis.

---

## ✨ Key features

| Module                     | What it does                                                                                                                                                      | Key file(s)                                                  |
|----------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------|
| **High-freq P2P capture**  | Pulls every Binance P2P advert (buy & sell) every 5 minutes; filters misleading users with `blocked_users.json`; builds position- & volume-weighted PVWAP + depth | `utils/scrapers/binance_request.py`                          |
| **CMV bank quote scraper** | Collects the Comisión Máxima Variable surcharge published by Bolivian banks                                                                                       | `utils/scrapers/cmv_request.py`                              |
| **TradingView bridge**     | Daily official FX & USDT/ARS backup feed via `tvdatafeed`                                                                                                         | `utils/scrapers/tradingview_request.py`                      |
| **Newspaper pipeline**     | 11 bespoke scrapers → HTML snapshots → LLM extraction of street quotes                                                                                            | `utils/scrapers/newspapers/*` · `utils/llm_processing.py`    |
| **ETL orchestrator**       | Runs everything on a single loop between 07:00-23:59 GMT-4, rolls up daily / monthly / quarterly averages overnight                                               | `main.py`                                                    |
| **Mongo controller**       | Auto-creates collections (`bolivian_blue_db`) and exposes Pandas helpers                                                                                          | `utils/mongo_controller.py`                                  |
| **Graph kit**              | Generates liquidity-depth & time-series PNGs used in the thesis and the companion website                                                                         | `notebooks/thesis_graphs.ipynb` · `utils/graph_generator.py` |

---

## 🗂 Repository layout

```
.
├── main.py                 # single-entry ETL loop
├── config.py               # dirs, record interval, DB host/port…
├── settings.json           # pairs & symbols to track
├── requirements.txt
├── utils/                  # package with all helpers
│   ├── scrapers/           # Binance, CMV, TradingView, newspapers
│   ├── data_processing.py  # PVWAP, outlier removal, blocked users
│   ├── llm_processing.py   # 2-stage OpenAI pipeline (NER → price)
│   └── ...
├── notebooks/              # Jupyter notebooks that reproduce thesis figs
└── data/                   # created at runtime (snapshots, graphs, cmv…)
```

---

## 🚨 Purpose

This repository contains the code used to collect and process the data for the thesis. Although the code could run as is
in any machine, the intention is to demonstrate the methodology and workflow used to gather the data, rather than to
provide a ready-to-use solution.

Nonetheless, the code can be used to collect the same data as in the thesis, for further research or analysis.

## 📜 License

MIT – see `LICENSE` for full text.  
If you use this code in academic work, please cite the thesis:

```
Farfan Rodriguez, L. (2025).
“Uncovering the Currency Price in an Unsustainable Fixed Exchange-Rate Regime:
  Using a Stablecoin – The Case of Bolivia.”
BSc Thesis, Luiss Guido Carli University.
```

---

_Author • Leonardo Farfan Rodriguez  
Supervised by Dr. Diletta Topazio_
