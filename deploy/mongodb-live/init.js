db = db.getSiblingDB("ledger");
db.createCollection("invoices");
db.createCollection("payments");
