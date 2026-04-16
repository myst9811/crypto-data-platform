---
type: community
cohesion: 0.67
members: 3
---

# Medallion Architecture Docs

**Cohesion:** 0.67 - moderately connected
**Members:** 3 nodes

## Members
- [[Citation Armbrust et al. Delta Lake VLDB 2020]] - document - REPORT_BRIEF.md
- [[Medallion Architecture (BronzeSilverGold)]] - document - ARCHITECTURE.md
- [[Rationale Medallion Architecture on Single Node]] - document - REPORT_BRIEF.md

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Medallion_Architecture_Docs
SORT file.name ASC
```
