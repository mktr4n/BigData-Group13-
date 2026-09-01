"""
discover_mongo.py

Verification script. Prints every database, every collection, document counts,
and the top-level field names of one sample document. Run this after the
mongoimport step to confirm the register data loaded correctly.

Run from a terminal on the host:

    docker exec group13_jupyter python /home/jovyan/work/discover_mongo.py

Or from a JupyterLab terminal:

    python /home/jovyan/work/discover_mongo.py
"""

from pymongo import MongoClient

# "mongodb" is the docker-compose service name. Containers on the same network
# reach each other by service name, so this works from the Jupyter container.
# From the host machine instead, use "mongodb://localhost:27018".
MONGO_URI = "mongodb://mongodb:27017"

SYSTEM_DBS = {"admin", "config", "local"}

client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)

# Fail loudly and immediately if Mongo is unreachable, rather than hanging.
client.admin.command("ping")
print("Connected to %s\n" % MONGO_URI)

for db_name in sorted(client.list_database_names()):
    if db_name in SYSTEM_DBS:
        continue
    db = client[db_name]
    print("DATABASE: %s" % db_name)

    for coll_name in sorted(db.list_collection_names()):
        coll = db[coll_name]
        # estimated_document_count() reads collection metadata rather than
        # counting documents, so it is fast but not guaranteed exact.
        count = coll.estimated_document_count()
        print("   collection: %-28s  ~%d documents" % (coll_name, count))

        doc = coll.find_one()
        if doc:
            fields = sorted(k for k in doc.keys())
            print("      top-level fields (%d): %s" % (len(fields), ", ".join(fields)))

            # Identify the Brreg register collection by the presence of fields
            # unique to it, then report the AS share with an exact count.
            required = ["organisasjonsform", "vedtektsfestetFormaal", "aktivitet"]
            missing = [f for f in required if f not in doc]
            if not missing:
                n_as = coll.count_documents({"organisasjonsform.kode": "AS"})
                print("      -> Brreg register collection. AS records: %d (%.1f%% of %d)"
                      % (n_as, n_as / count * 100 if count else 0, count))
            else:
                print("      -> not the Brreg register collection (missing: %s)"
                      % ", ".join(missing))
        print()
    print()

client.close()
print("Expected after a successful load: database 'companiesdb' with")
print("collections 'companies' (~1,171,373 docs) and 'financial_data'.")
