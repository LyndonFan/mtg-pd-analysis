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
select
"cardName",
"archetype",
"numDecks",
"numDecks"::float / other."numArchetypeDecks" as "avgIncludeRate",
n::float / "numDecks" as "avgIncludeN",
CASE
    WHEN matches = 0 THEN 0
    ELSE wins::float / matches
END AS "winRate",
(wins::float+1.0)/(matches+2.0) as "smoothWinRate"
from grouped join (
    select b.*, archetype from
    (
        select "archetypeId", count(1) as "numArchetypeDecks"
        from decks group by "archetypeId"
    ) b
    join archetypes
    on b."archetypeId" = archetypes.id
) other
on grouped."archetypeId" = other."archetypeId"