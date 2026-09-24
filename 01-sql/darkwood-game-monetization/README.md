# Монетизация онлайн-игры «Секреты Тёмнолесья»

**Задача.** Изучить, как характеристики игроков и их персонажей влияют на покупку внутриигровой валюты, и оценить активность игроков при покупках.

## Что сделано

- доля платящих игроков — в целом и в разрезе рас персонажей;
- статистика стоимости покупок, поиск аномальных нулевых покупок;
- популярность эпических предметов;
- ad hoc-анализ: зависимость покупательской активности от расы персонажа (многоуровневые CTE).

**Стек:** PostgreSQL: CTE, подзапросы, агрегатные функции, JOIN

**Решение:** [`darkwood_analysis.sql`](darkwood_analysis.sql)

---

## In English

**In-game currency monetization analysis ("Darkwood Secrets")**

SQL research into how player and character attributes affect purchases of in-game currency: payer share by character race, purchase statistics, anomalous zero-cost purchases, popular epic items, and an ad hoc analysis of purchasing activity by race built with multi-level CTEs.

**Key result:** Payer and purchase metrics by character race

_Notebooks and code comments are in Russian._
