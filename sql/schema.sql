CREATE TABLE Questions (
    Id SERIAL,
    OwnerUserId INTEGER,
    CreationDate TIMESTAMP NOT NULL,
    Score BIGINT NOT NULL,
    Title VARCHAR(255) NOT NULL,
    Body TEXT NOT NULL,
    PRIMARY KEY (Id)
);

CREATE TABLE Answers (
    Id SERIAL,
    OwnerUserId INTEGER,
    ParentId INTEGER NOT NULL,
    CreationDate TIMESTAMP NOT NULL,
    Score INT NOT NULL,
    Body TEXT NOT NULL,
    PRIMARY KEY (Id),
    FOREIGN KEY (ParentId) REFERENCES Questions(Id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE Tags (
    Id INTEGER NOT NULL,
    Tag VARCHAR(255) NOT NULL,
    PRIMARY KEY (Id, Tag),
    FOREIGN KEY (Id) REFERENCES Questions(Id) ON DELETE CASCADE ON UPDATE CASCADE
);