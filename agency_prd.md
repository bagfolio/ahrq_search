The **AHRQ Compendium of U.S. Health Systems** (and its yearly updates and PDF data briefs) is widely cited in health-services research, but AHRQ’s internal list of articles stops at 2021\. Our task is to build a repeatable pipeline that **finds every peer-reviewed paper that *uses* data from the Compendium—not just those that mention it in passing—going forward from 2016 through the present.**

Why it matters

* Managers need an up-to-date impact tally for grant justifications and congressional briefs.

* Researchers need to locate prior analyses that merged Compendium data with other datasets (e.g., Medicare, HIE adoption).

* No DOI exists for most Compendium releases; many authors cite raw URLs or the agency name, so simple DOI citation counts miss the majority of real uses.

---

#### **2 Objectives & Success Criteria**

| Objective | Measure of success |
| ----- | ----- |
| **Comprehensive retrieval** | ≥ 95 % of articles identified in a blinded hand audit of 2023–2024 literature. |
| **Usage vs. mention classification** | ≥ 90 % precision in flagging “data used” after regex pass (stretch: ≥ 95 % with a lightweight spaCy classifier). |
| **Hands-off refresh** | One command (or scheduled action) produces updated CSVs in \< 15 min on a laptop and e-mails a summary to project leads. |
| **Transparent evidence** | Each hit is stored with DOI/PMID, title, abstract, the matched keyword, and (if OA) the paragraph snippet that triggered the “used data” flag. |

---

#### **3 Data Sources & Why Chosen**

1. **PubMed (via PyMed)** – core index for U.S. health-services and clinical journals.

2. **OpenAlex (via PyAlex)** – captures journals PubMed misses (e.g., *Health Affairs*, *Social Science & Medicine*) and provides open-access links.

3. **NIH Open Citation Collection (via pmidcite)** – supplies the forward citation network of the Compendium’s methods paper (PMID 30674227).

4. **Google Scholar (via scholarly)** – last-resort catch-all for conference papers, institutional repositories, and pre-prints.

5. **Full-text retrieval** – • OA PDFs from OpenAlex *→* Trafilatura; • Unpaywall fallback for closed-access DOIs.

6. **AHRQ site map URLs & PDF filenames** – treated as first-class keywords because many papers paste the raw link rather than a formal citation.

---

#### **4 Keyword Strategy (core to recall)**

* **Exact URL strings** – every Compendium landing page and each `chsp-brief*.pdf` link.

* **Formal titles** – “Compendium of U.S. Health Systems”, “Compendium of US Health Systems”.

* **Agency \+ dataset combos** – “AHRQ Compendium”, “AHRQ health system compendium”.

* **Year-specific phrases** – “Compendium 2023 AHRQ”, “Compendium 2021 health systems”.

* **Funding acknowledgments** – “Agency for Healthcare Research and Quality” **AND** Compendium.

*All keywords are stored in `keywords.yml` so collectors read a single source of truth.*

---

#### **5 System Architecture Overview**

pgsql  
CopyEdit  
`[ keywords.yml ]`  
       `|`  
`╭─────────────────────────────────────────╮`  
`│   Collector Orchestrator (Python CLI)  │  ← Academic-Tracker style runner`  
`╰─────────────────────────────────────────╯`  
   `|       |          |            |`  
 `PyMed   PyAlex   pmidcite   scholarly`  
`(PubMed) (OpenAlex) (NIH OCC) (Scholar)`  
   `|       |          |            |`  
   `|    JSON/DFs      |         JSON`  
   `└─────────────── master dataframe ────────────────┐`  
                                                     `│`  
                                      `OA full-text fetch (Trafilatura + Unpaywall)`  
                                                     `│`  
                       `usage-vs-mention classification (regex → spaCy upgrade)`  
                                                     `│`  
                             `┌──────────────┬────────┘`  
                          `CSV export    Summary HTML report`

*All components are pip-installable; no headless browser or Selenium unless Scholar scraping becomes unreliable.*

---

#### **6 Core Components & Implementation Notes**

##### **6.1 Collector Orchestrator**

* Reads `keywords.yml`.

* Spins worker threads to query each source.

* Normalises fields (`doi`, `pmid`, `title`, `year`).

* Dedupe rule: lowest-case DOI else normalised title.

* Persists raw pulls (for audit) and the merged master CSV.

##### **6.2 Source-specific adapters**

* **PyMed** – uses NCBI ESearch → EFetch; auto-throttles at 3 requests/second.

* **PyAlex** – paginated REST; 200/req, 100 k daily quota.

* **pmidcite** – one call gets all forward citations; include reference list if `--backrefs` needed.

* **scholarly** – run on a small rotating proxy; cap to 100 results per query; sleep 30 s per page.

##### **6.3 Full-Text Retrieval**

* If OpenAlex returns `primary_location.source.fulltext_url` use it.

* Else call Unpaywall (`GET https://api.unpaywall.org/v2/{doi}?email=…`).

* Trafilatura `fetch_url(url, include_comments=False)` to strip boilerplate.

* Store SHA256 hash of text to avoid re-processing duplicates.

##### **6.4 Usage Detection**

* Regex pass flags obvious uses (`used … compendium`, URL embeds).

* **Stretch**: fine-tune `en_core_web_sm` text-cat on 30 tagged examples; expected F1 ≈ 0.93.

##### **6.5 Outputs & Reporting**

* `all_hits.csv` – every candidate with fields: DOI, PMID, title, year, source, match\_keyword, oa\_url, uses\_compendium (0/1), snippet (first 160 chars around match).

* `used_compendium.csv` – filtered subset.

* `report.html` – bar chart (matplotlib) papers-per-year and table of top journals.

* Optional: push CSV to Google Sheets via `gspread` for stakeholder browsing.

---

#### **7 Milestones & Timeline**

| Week | Deliverable |
| ----- | ----- |
| 1 | Finalize `keywords.yml`; stub collector returning raw JSON from each source. |
| 2 | Merge/dedupe logic; master CSV builds end-to-end; regex usage flag. |
| 3 | OA full-text pipeline \+ Trafilatura; precision audit on 20 articles. |
| 4 | spaCy text-cat prototype; add HTML report. |
| 5 | GitHub Action nightly run; Google-Sheets sync; documentation. |
| 6 | Stakeholder review; tweak thresholds; hand-audit recall; v1.0 release. |

---

#### **8 Risks & Mitigations**

* **Google Scholar blocking** – rotate IPs or switch to paid SerpAPI.

* **Closed-access PDFs** – flag as “verification needed”; manual QA monthly.

* **New Compendium releases** – cron job checks AHRQ site map weekly and auto-appends URLs to `keywords.yml`.

* **API schema changes** – pin library versions; unit tests on adapters.

