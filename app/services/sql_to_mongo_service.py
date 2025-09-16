from app.infrastructure.sql_to_mongo_repository import SQLToMongoRepository


class SQLToMongoService:
    def __init__(self, repository: SQLToMongoRepository):
        self.repository = repository

    def migrate_table(self, table_name: str) -> dict:
        return self.repository.migrate_table(table_name)
