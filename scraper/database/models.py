import enum
from typing import Optional
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy import String, SmallInteger, DateTime, Enum
from sqlalchemy import ForeignKey, CheckConstraint


class Base(DeclarativeBase):
    pass


class Source(enum.Enum):
    League = "League"
    Gatherling = "Gatherling"

    @classmethod
    def convert(cls, value: str) -> Optional["Source"]:
        if value == Source.League.value:
            return Source.League
        elif value == Source.Gatherling.value:
            return Source.Gatherling
        else:
            return None


class Person(Base):
    __tablename__ = "people"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
    name: Mapped[str] = mapped_column(String(64), nullable=False)
    decks: Mapped[list["Deck"]] = relationship(back_populates="people")


class Archetype(Base):
    __tablename__ = "archetypes"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
    archetype: Mapped[str] = mapped_column(String(64), nullable=False)
    decks: Mapped[list["Deck"]] = relationship(back_populates="archetypes")


class Deck(Base):
    __tablename__ = "decks"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
    name = mapped_column(String)
    maindeck: Mapped["Maindeck"] = relationship(back_populates="deck")
    sideboard: Mapped["Sideboard"] = relationship(back_populates="deck")
    colorHasW: Mapped[bool]
    colorHasU: Mapped[bool]
    colorHasB: Mapped[bool]
    colorHasR: Mapped[bool]
    colorHasG: Mapped[bool]
    colorHasC: Mapped[bool]
    createdDatetime = mapped_column(DateTime)
    updatedDatetime = mapped_column(DateTime)
    personId: Mapped[int] = mapped_column(ForeignKey("people.id"))
    people: Mapped["Person"] = relationship(back_populates="decks")
    seasonId: Mapped[int] = mapped_column(SmallInteger)
    sourceName = mapped_column(Enum(Source))
    url: Mapped[str] = mapped_column(String(256))
    archetypeId: Mapped[int] = mapped_column(ForeignKey("archetypes.id"))
    archetypes: Mapped["Archetype"] = relationship(back_populates="decks")
    competitionId: Mapped[int] = mapped_column(SmallInteger)
    finish: Mapped[Optional[int]] = mapped_column(SmallInteger, nullable=True)
    retired: Mapped[bool]
    wins: Mapped[int] = mapped_column(SmallInteger)
    losses: Mapped[int] = mapped_column(SmallInteger)
    draws: Mapped[int] = mapped_column(SmallInteger)
    matches: Mapped[int] = mapped_column(SmallInteger)
    omwPercent: Mapped[Optional[int]] = mapped_column(SmallInteger, nullable=True)


class Maindeck(Base):
    __tablename__ = "maindecks"
    id: Mapped[int] = mapped_column(primary_key=True)
    deckId: Mapped[int] = mapped_column(ForeignKey("decks.id"))
    deck: Mapped["Deck"] = relationship(back_populates="maindeck")
    n: Mapped[int] = mapped_column(SmallInteger)
    name: Mapped[str] = mapped_column(String(128))


class Sideboard(Base):
    __tablename__ = "sideboards"
    id: Mapped[int] = mapped_column(primary_key=True)
    deckId: Mapped[int] = mapped_column(ForeignKey("decks.id"))
    deck: Mapped["Deck"] = relationship(back_populates="sideboard")
    n: Mapped[int] = mapped_column(SmallInteger)
    name: Mapped[str] = mapped_column(String(128))


# create tables (from scratch)
# requires those tables to not exist in the first place
if __name__ == "__main__":
    from .database import Database

    db = Database()
    from sqlalchemy import create_engine

    engine = create_engine("postgresql+psycopg2://", creator=db.connection, echo=True)
    Base.metadata.create_all(engine)
