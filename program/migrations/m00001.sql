CREATE TABLE position
(
    id                 INTEGER PRIMARY KEY, -- Auto-incremented by SQLite automatically
    base_currency      TEXT           NOT NULL,
    quote_currency     TEXT           NOT NULL,
    secondary_currency TEXT           NOT NULL,
    open_position      DECIMAL(18, 8) NOT NULL,
    closed_position    DECIMAL(18, 8),
    open_timestamp     DATETIME       NOT NULL DEFAULT (datetime('now')),
    close_timestamp    DATETIME                DEFAULT NULL,
    UNIQUE (base_currency, quote_currency, secondary_currency, open_timestamp)
);
