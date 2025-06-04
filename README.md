# Uncovering the Currency Price in an Unsustainable Fixed Exchange Rate Regime Using a Stablecoin: The Case of Bolivia

_A companion repository for the BSc thesis  
“Uncovering the Currency Price in an Unsustainable Fixed Exchange-Rate Regime”_

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

## 🔧 Quick start

### 1 · Prerequisites

* **Python 3.10 – 3.12**
* **MongoDB 6+** running locally or reachable over the network  
  Default settings are `localhost:27017`; edit `MONGO_HOST`, `MONGO_PORT` in `config.py` if needed.

### 2 · Installation

```bash
git clone https://github.com/your-handle/bolivian_blue.git
cd bolivian_blue

# create & activate a virtual env
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\Activate

# install deps (≈ 150 packages inc. tvdatafeed, pymongo, bs4, openai …)
pip install --upgrade pip
pip install -r requirements.txt
```

### 3 · First run

```bash
# make sure MongoDB is up, then:
python main.py
```

*The loop will stay alive between **07:00 and 23:59 America/La_Paz**.  
Outside that window it fetches end-of-day sources, rolls up aggregates and exits.*

> **Debug mode** – to dump every raw response for troubleshooting:
> ```python
> from main import main
> main(debug=True)
> ```

## ⚙️ Configuration knobs

| File                       | What to edit                                              |
|----------------------------|-----------------------------------------------------------|
| `settings.json`            | Add new Binance fiat pairs or TradingView tickers         |
| `utils/blocked_users.json` | Append bad actors’ Binance nicknames                      |
| `config.py`                | Folder locations, record interval (`RECORD_INTERVAL = 5`) |

---

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
