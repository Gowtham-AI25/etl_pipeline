# Entities
---

### 🔹 `institutions`

* **Description**: Represents academic, government, and corporate organizations affiliated with research works (e.g., universities, labs, think tanks).
* **Purpose**: Helps analyze research output and impact at the organizational level.
* **Usage**:

  * Ranking institutions based on publication and citation metrics.
  * Studying collaboration patterns (e.g., inter-institutional co-authorship).
  * Mapping research funding flows or geographic distribution.

---

### 🔹 `sources`

* **Description**: Represents journals, conferences, or repositories that publish scholarly work.
* **Purpose**: Critical for bibliometric analysis and evaluating dissemination channels.
* **Usage**:

  * Analyzing publishing trends and journal impact.
  * Identifying dominant sources in specific research fields.
  * Filtering papers by source quality or open access availability.

---

### 🔹 `funders`

* **Description**: Organizations that provide financial support for research (e.g., NSF, NIH, World Bank).
* **Purpose**: Captures the funding landscape behind scholarly output.
* **Usage**:

  * Evaluating how funding correlates with research impact.
  * Tracking funder influence over time or by topic.
  * Policy insights for research investment strategies.

---

### 🔹 `concepts`

* **Description**: Hierarchical topics or fields that categorize scholarly work (e.g., "Machine Learning", "Finance").
* **Purpose**: Enables thematic classification and topical analysis.
* **Usage**:

  * Tagging works by subject area.
  * Constructing topic trends or clustering.
  * Filtering and ranking works by research domain.

---

### 🔹 `works` (aka papers)

* **Description**: Core entity representing individual scholarly articles, preprints, reports, or conference papers.
* **Purpose**: Central object in OpenAlex—everything links back to a research work.
* **Usage**:

  * Citation analysis, topic modeling, or trend forecasting.
  * Author, institution, and funder attribution.
  * Building recommendation engines and knowledge graphs.

---

### 🔹 `authors` (if included)

* **Description**: Represents individual researchers who contribute to scholarly works.
* **Purpose**: Key for profiling research careers and collaboration networks.
* **Usage**:

  * Author-level impact analysis (h-index, total citations).
  * Co-authorship networks and influence mapping.
  * Career trajectory and productivity studies.

---

### 🔹 `venues` / `publishers` (if modeled separately)

* **Description**: Higher-level entity representing the publisher of sources.
* **Purpose**: Useful for grouping and analyzing journals by publisher ownership.
* **Usage**:

  * Market share studies of academic publishing.
  * Analyzing open access strategies across publishers.

---
# OpenAlex Financial Papers Database – Schema Overview

---

## Tables and Relationships

### 📄 **papers\_main**

* **PK**: `paper_id`
* Describes the core metadata of a research paper.

### 📊 **papers\_citation\_data**

* **PK**: `paper_id`
* **FK**: `paper_id` → `papers_main.paper_id`

### 🔗 **papers\_reference\_ids**

* **PK**: `paper_id`
* **FK**: `paper_id` → `papers_citation_data.paper_id`

### 🔁 **papers\_related\_work\_ids**

* **PK**: `paper_id`
* **FK**: `paper_id` → `papers_citation_data.paper_id`

### 👤 **papers\_author\_data**

* **PK**: `author_id`

### 👥 **papers\_paper\_author**

* **Composite Key**: (`paper_id`, `author_id`) \[optional]
* **FKs**:

  * `paper_id` → `papers_main.paper_id`
  * `author_id` → `papers_author_data.author_id`

### 📰 **papers\_publication\_data**

* **PK**: `paper_id`
* **FK**: `paper_id` → `papers_main.paper_id`
* **FK**: `primary_source_id` → `sources_main.source_id`

### 🧭 **papers\_indexed\_in**

* **FK**: `paper_id` → `papers_main.paper_id`

### 🔓 **papers\_opensource\_access**

* **PK**: `paper_id`
* **FKs**:

  * `paper_id` → `papers_main.paper_id`
  * `best_oa_source_id` → `sources_main.source_id`

### 🧠 **papers\_topics\_data**

* **PK**: `paper_id`
* **FK**: `paper_id` → `papers_main.paper_id`

### 🧩 **papers\_all\_topics**

* **FK**: `paper_id` → `papers_main.paper_id`

### 🏷️ **papers\_keywords**

* **FK**: `paper_id` → `papers_main.paper_id`

### 🧬 **papers\_concepts**

* **FKs**:

  * `paper_id` → `papers_main.paper_id`
  * `concept_id` → `concepts_main.concept_id`

### 📥 **papers\_paper\_availability**

* **FKs**:

  * `paper_id` → `papers_main.paper_id`
  * `location_source_id` → `sources_main.source_id`

---

### 🧭 **concepts\_main**

* **PK**: `concept_id`

### 🔗 **concepts\_ancestor\_concepts**

* **Composite Key**: (`concept_id`, `ancestor_con_id`)
* **FKs**:

  * `concept_id` → `concepts_main.concept_id`
  * `ancestor_con_id` → `concepts_main.concept_id`

### 🔗 **concepts\_related\_concepts**

* **Composite Key**: (`concept_id`, `related_concepts_id`)
* **FKs**:

  * `concept_id` → `concepts_main.concept_id`
  * `related_concepts_id` → `concepts_main.concept_id`

### 📈 **concepts\_counts\_by\_year**

* **Composite Key**: (`concept_id`, `year`)
* **FK**: `concept_id` → `concepts_main.concept_id`

---

### 💰 **funders\_main**

* **PK**: `funder_id`

### 📈 **funder\_counts\_by\_years**

* **Composite Key**: (`funder_id`, `year`)
* **FK**: `funder_id` → `funders_main.funder_id`

### 🆎 **funder\_alt\_titles**

* **FK**: `funder_id` → `funders_main.funder_id`

### 🧪 **funder\_extra\_roles**

* **Composite Key**: (`funder_id`, `role`, `id`)
* **FK**: `funder_id` → `funders_main.funder_id`

### 🔬 **funder\_associated\_concepts**

* **Composite Key**: (`funder_id`, `id`)
* **FK**: `funder_id` → `funders_main.funder_id`

---

### 🗞️ **sources\_main**

* **PK**: `source_id`

### 📅 **sources\_counts\_by\_year**

* **Composite Key**: (`source_id`, `year`)
* **FK**: `source_id` → `sources_main.source_id`

### 🧠 **sources\_associated\_concepts**

* **Composite Key**: (`source_id`, `concept_id`)
* **FK**: `source_id` → `sources_main.source_id`

### 🧬 **sources\_topic\_share**

* **Composite Key**: (`source_id`, `topic_id`)
* **FK**: `source_id` → `sources_main.source_id`

### 🧵 **sources\_associated\_topics**

* **Composite Key**: (`source_id`, `topic_id`)
* **FK**: `source_id` → `sources_main.source_id`

### 🔖 **sources\_alternate\_titles**

* **PK**: `index`
* **FK**: `source_id` → `sources_main.source_id`

### 🧬 **sources\_host\_org\_lineage\_ids**

* **FK**: `source_id` → `sources_main.source_id`

---

### 🏫 **institution\_main**

* **PK**: `institution_id`

### 🏢 **institution\_alternatives\_names**

* **FK**: `institution_id` → `institution_main.institution_id`

### 🧪 **institution\_institution\_roles**

* **Composite Key**: (`institution_id`, `role`, `id`)
* **FK**: `institution_id` → `institution_main.institution_id`

### 🆎 **institution\_acronyms**

* **FK**: `institution_id` → `institution_main.institution_id`

### 🔁 **institution\_lineage\_ids**

* **FK**: `institution_id` → `institution_main.institution_id`

### 🤝 **institution\_related\_institutions**

* **Composite Key**: (`institution_id`, `id`)
* **FK**: `institution_id` → `institution_main.institution_id`

### 🌍 **institution\_geo\_data**

* **PK**: `geonames_city_id`

### 📍 **institution\_geo\_id**

* **Composite Key**: (`institution_id`, `geonames_city_id`)
* **FKs**:

  * `institution_id` → `institution_main.institution_id`
  * `geonames_city_id` → `institution_geo_data.geonames_city_id`

### 📊 **institution\_counts\_by\_year**

* **Composite Key**: (`institution_id`, `year`)
* **FK**: `institution_id` → `institution_main.institution_id`

### 🧠 **institution\_associated\_concepts**

* **Composite Key**: (`institution_id`, `concept_id`)
* **FK**: `institution_id` → `institution_main.institution_id`

### 🧬 **institution\_associated\_topics**

* **Composite Key**: (`institution_id`, `ass_topic_id`)
* **FK**: `institution_id` → `institution_main.institution_id`

### 🔗 **institution\_topic\_share**

* **Composite Key**: (`institution_id`, `topic_id`)
* **FK**: `institution_id` → `institution_main.institution_id`

---

### 🧾 **lookup\_main**

* **PK**: `topic_id`

### 📋 **lookup\_subfield\_data**, **lookup\_field\_data**, **lookup\_domain\_data**

* **PKs**: `subfield_id`, `field_id`, `domain_id` respectively

### 🔁 **lookup\_sibling\_topics**

* **Composite Key**: (`topic_id`, `sibling_id`)
* **FKs**:

  * `topic_id` → `lookup_main.topic_id`
  * `sibling_id` → `lookup_main.topic_id`

### 🧷 **lookup\_topic\_keywords**

* **FK**: `topic_id` → `lookup_main.topic_id`

### 🧭 **lookup\_parent\_lookup**

* **PK**: `topic_id`
* **FKs**:

  * `topic_id` → `lookup_main.topic_id`
  * `subfield` → `lookup_subfield_data.subfield_id`
  * `field` → `lookup_field_data.field_id`
  * `domain` → `lookup_domain_data.domain_id`

---
