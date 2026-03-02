"""Load 3 demo knowledge groups into the system."""

import asyncio
import logging
from logging import getLogger

from app.common.mongo import get_mongo_client
from app.config import config

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = getLogger(__name__)

COLLECTION = "knowledgeGroups"
DEV_USER_ID = "local-dev-test"

DEMO_GROUPS = [
    {
        "name": "DEFRA Policy Documents",
        "description": "Core policy and guidance documents",
    },
    {
        "name": "Environmental Regulations",
        "description": "UK environmental legislation and compliance",
    },
    {
        "name": "Farming & Agriculture",
        "description": "Agricultural guidance and best practices",
    },
]


async def load_demo_knowledge_groups() -> None:
    client = await get_mongo_client()
    db = client.get_database(config.mongo_database)
    collection = db[COLLECTION]

    await collection.create_index(
        [("created_by", 1), ("name", 1)],
        unique=True,
    )

    for group in DEMO_GROUPS:
        doc = {
            "name": group["name"],
            "description": group["description"],
            "created_by": DEV_USER_ID,
        }
        result = await collection.update_one(
            {"created_by": DEV_USER_ID, "name": group["name"]},
            {"$set": doc},
            upsert=True,
        )
        if result.upserted_id:
            logger.info(
                "Inserted demo knowledge group: %s (id=%s)",
                group["name"],
                result.upserted_id,
            )
        else:
            logger.info("Updated existing demo knowledge group: %s", group["name"])

    await client.close()
    logger.info("Loaded %d demo knowledge groups", len(DEMO_GROUPS))


def main() -> None:
    asyncio.run(load_demo_knowledge_groups())


if __name__ == "__main__":
    main()
