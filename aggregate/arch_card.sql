with joined as (
    select decks.*, {table}.n,
    {table}.name as "cardName"
    from decks join {table}
    on decks.id = {table}."deckId"
),
grouped as (
    select
    "cardName",
    "archetypeId",
    sum(n) as n,
    count(1) as "numDecks",
    sum(wins) as wins,
    sum(matches) as matches
    from joined
    group by "cardName", "archetypeId"
)
select *,
CASE
    WHEN matches = 0 THEN 0
    ELSE wins::float / matches
END AS "winRate",
(wins::float+1.0)/(matches+2.0) as "smoothWinRate"
from grouped