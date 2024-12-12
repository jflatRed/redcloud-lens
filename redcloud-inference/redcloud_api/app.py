import json
from beam import endpoint, Image
import snowflake.connector as sf
from typing import Any
from pydantic import BaseModel


class InferenceRequest(BaseModel):
    label: str
    country: str | None = None
    limit: int


# class InferenceResponse(BaseModel):
#     label: str
#     confidence: float
#     class_id: int
# predictions: List[Dict[str, Any]]


from venv import logger
import boto3
from botocore.exceptions import ClientError
import pymysql
import os

# DB_ENDPOINT = os.environ["DB_ENDPOINT"]
# DB_USER = os.environ["DB_USER"]
# DB_PORT = int(os.environ["DB_PORT"])
# # DB_REGION = os.environ["DB_REGION"]
# DB_TOKEN = os.environ["DB_TOKEN"]
# DB_NAME = os.environ["DB_NAME"]


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
                host="lensredcloud.c6tdyc2jwuyb.us-east-1.rds.amazonaws.com",
                user="admin",
                password="12345678",
                port=3306,
                database="redcloud-lens",
                # region="us-east-1",
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
            region_name="us-east-1",
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


@endpoint(
    cpu=1,
    name="redcloud-api",
    image=Image(
        python_packages=["pymysql", "snowflake-connector-python", "pydantic", "boto3"],
        python_version="python3.11",
    ),
    secrets=["DB_ENDPOINT", "DB_USER", "DB_PORT", "DB_REGION, DB_TOKEN, DB_NAME"],
)
def handle_request(**args: InferenceRequest) -> Any:
    try:
        validated = InferenceRequest(**args)
    except Exception as e:
        return {"error": str(e)}

    db = InstanceWrapper.from_client()
    # WHERE Country='Nigeria'
    query = f"""
        SELECT Brand, `Brand_Manufacturer`, `CategoryName`, `Country`, `HSRecordID`, `LastPriceUpdateAt`, `Manufacturer`, `ProductCreationDate`, `ProductID`, `ProductName`, `ProductPrice`, `ProductStatus`, `Quantity`, `SalableQuantity`, `SellerGroup`, `SellerID`, `SellerName`, `SKU`, `StockStatus`, `TopCategory`
        FROM Products
        where ProductName LIKE '%{validated.label}%'
        {f"and Country='{validated.country}'" if validated.country else ""}
        # AND StockStatus='In Stock'
        ORDER BY ProductPrice ASC
        limit {validated.limit}
        
        """
    data = db.query_db(query)
    data = [
        {
            "Brand": str(i[0]),
            "Brand_Manufacturer": str(i[1]),
            "CategoryName": str(i[2]),
            "Country": str(i[3]),
            "HSRecordID": str(i[4]),
            "LastPriceUpdateAt": str(i[5]),
            "Manufacturer": str(i[6]),
            "ProductCreationDate": str(i[7]),
            "ProductID": str(i[8]),
            "ProductName": str(i[9]),
            "ProductPrice": str(i[10]),
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
    return {
        "data": data,
        "message": "Success",
        "label": validated.label,
    }


if __name__ == "__main__":
    value = handle_request(label="Coca cola", country="Nigeria", limit=10)
    print(value)
