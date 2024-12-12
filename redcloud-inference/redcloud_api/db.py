# import snowflake.connector as sf


# class DB:
#     conn: sf.SnowflakeConnection

#     def __init__(self):
#         self.conn = sf.connect(
#             account="cc88289.eu-west-1",
#             user="joshua.eseigbe@redcloudtechnology.com",
#             authenticator="externalbrowser",
#             role="POC_RECOMMENDATION_USER_DEMO_GROUP",
#             warehouse="DEV_ML",
#             database="PRD_SANDBOX",
#             schema="PUBLIC",
#         )

#     def run_query(self, query):
#         """
#         Run a query on the Snowflake database
#         :param query: The query to run
#         """
#         cursor = self.conn.cursor()

#         try:
#             cursor.execute(query)
#             result = cursor.fetchall()
#             return result
#         finally:
#             cursor.close()
#             self.conn.close()


# # Create a cursor object to run queries


# if __name__ == "__main__":
#     db = DB()
#     query = """
#         SELECT *
#         FROM MARKETPLACE_PRODUCT
#         WHERE "Country"='Nigeria'
#         AND "Product Name" LIKE 'Coca-cola%'
#         AND "Stock Status"='In Stock'
#         ORDER BY "Product Price" ASC
#         """
#     result = db.run_query(query)
#     print(result)
from venv import logger
import boto3
from botocore.exceptions import ClientError
import pymysql
import os

DB_ENDPOINT = os.environ["DB_ENDPOINT"]
DB_USER = os.environ["DB_USER"]
DB_PORT = int(os.environ["DB_PORT"])
DB_REGION = os.environ["DB_REGION"]
DB_TOKEN = os.environ["DB_TOKEN"]
DB_NAME = os.environ["DB_NAME"]


class InstanceWrapper:
    """Encapsulates Amazon RDS DB instance actions."""

    def __init__(self, rds_client):
        """
        :param rds_client: A Boto3 Amazon RDS client.
        """
        self.rds_client = rds_client
        # Generate an auth token for the DB instance
        try:
            conn = pymysql.connect(
                # auth_plugin_map={"mysql_clear_password": None},
                host=DB_ENDPOINT,
                user=DB_USER,
                password=DB_TOKEN,
                port=DB_PORT,
                database=DB_NAME,
                # ssl_ca="SSLCERTIFICATE",
                # ssl_verify_identity=True,
            )
            self.connection = conn

        except Exception as e:
            print("Database connection failed due to {}".format(e))
            raise e

    @classmethod
    def from_client(cls):
        """
        Instantiates this class from a Boto3 client.
        """
        # gets the credentials from .aws/credentials
        # session = boto3.Session(profile_name="default")
        rds_client = boto3.client(
            "rds",
        )
        return cls(rds_client)

    def query_db(self, query):
        """
        Queries the DB instance.

        :param query: The query to run.
        :return: The results of the query.
        """
        try:
            with self.connection.cursor() as cur:
                cur.execute(query)
                query_results = cur.fetchall()
                # print(query_results)
                return query_results
        except Exception as e:
            logger.error("Couldn't query the DB instance. Here's why: %s", e)
            raise

    def create_db_instance(
        self,
        db_name,
        instance_id,
        parameter_group_name,
        db_engine,
        db_engine_version,
        instance_class,
        storage_type,
        allocated_storage,
        admin_name,
        admin_password,
    ):
        """
        Creates a DB instance.

        :param db_name: The name of the database that is created in the DB instance.
        :param instance_id: The ID to give the newly created DB instance.
        :param parameter_group_name: A parameter group to associate with the DB instance.
        :param db_engine: The database engine of a database to create in the DB instance.
        :param db_engine_version: The engine version for the created database.
        :param instance_class: The DB instance class for the newly created DB instance.
        :param storage_type: The storage type of the DB instance.
        :param allocated_storage: The amount of storage allocated on the DB instance, in GiBs.
        :param admin_name: The name of the admin user for the created database.
        :param admin_password: The admin password for the created database.
        :return: Data about the newly created DB instance.
        """
        try:
            response = self.rds_client.create_db_instance(
                DBName=db_name,
                DBInstanceIdentifier=instance_id,
                DBParameterGroupName=parameter_group_name,
                Engine=db_engine,
                EngineVersion=db_engine_version,
                DBInstanceClass=instance_class,
                StorageType=storage_type,
                AllocatedStorage=allocated_storage,
                MasterUsername=admin_name,
                MasterUserPassword=admin_password,
            )
            db_inst = response["DBInstance"]
        except ClientError as err:
            logger.error(
                "Couldn't create DB instance %s. Here's why: %s: %s",
                instance_id,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise
        else:
            return db_inst


if __name__ == "__main__":
    instance = InstanceWrapper.from_client()
    data = instance.query_db(
        """
        SELECT Brand, `Brand_Manufacturer`, `CategoryName`, `Country`, `HSRecordID`, `LastPriceUpdateAt`, `Manufacturer`, `ProductCreationDate`, `ProductID`, `ProductName`, `ProductPrice`, `ProductStatus`, `Quantity`, `SalableQuantity`, `SellerGroup`, `SellerID`, `SellerName`, `SKU`, `StockStatus`, `TopCategory`
        FROM Products
        WHERE Country='Nigeria'
        AND ProductName LIKE 'Smirnoff%'
        AND StockStatus='In Stock'
        ORDER BY ProductPrice ASC
        limit 10
        
        """
    )
    data = [
        {
            "Brand": i[0],
            "Brand_Manufacturer": i[1],
            "CategoryName": i[2],
            "Country": i[3],
            "HSRecordID": i[4],
            "LastPriceUpdateAt": i[5],
            "Manufacturer": i[6],
            "ProductCreationDate": i[7],
            "ProductID": i[8],
            "ProductName": i[9],
            "ProductPrice": i[10],
            "ProductStatus": i[11],
            "Quantity": i[12],
            "SalableQuantity": i[13],
            "SellerGroup": i[14],
            "SellerID": i[15],
            "SellerName": i[16],
            "SKU": i[17],
            "StockStatus": i[18],
            "TopCategory": i[19],
        }
        for i in data
    ]
    print(data)
# AND "ProductName" LIKE 'Smirnoff%'
#         AND "StockStatus"='In Stock'
#         ORDER BY "ProductPrice" ASC
