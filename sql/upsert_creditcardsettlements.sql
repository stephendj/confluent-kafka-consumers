INSERT INTO "xendit-deposit-service".creditcardsettlements_full
(
    {keys}
)
VALUES
(
    {values}
)
ON CONFLICT (id) DO UPDATE SET {update_keys};